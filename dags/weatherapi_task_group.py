from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.providers.http.operators.http import HttpOperator
import pandas as pd
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def kelvin_to_celcius(temp_in_kelvin):
    temp_in_celcius = (temp_in_kelvin - 273.15) 
    return temp_in_celcius

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.extract_weather_data") 
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celcius = kelvin_to_celcius(data["main"]["temp"])
    feels_like_celcius= kelvin_to_celcius(data["main"]["feels_like"])
    min_temp_celcius = kelvin_to_celcius(data["main"]["temp_min"])
    max_temp_celcius = kelvin_to_celcius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']).strftime('%A, %B %d, %Y at %H:%M:%S %Z')
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']).strftime('%I:%M:%S %p %Z')
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']).strftime('%I:%M:%S %p %Z')
    conn = BaseHook.get_connection('s3_conn_weather')
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celcius,
                        "Feels Like (C)": feels_like_celcius,
                        "Minimun Temp (C)":min_temp_celcius,
                        "Maximum Temp (C)": max_temp_celcius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time            
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = json.loads(conn.extra)
    now = datetime.now()    
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_bandung_' + dt_string
    df_data.to_csv(f"s3://weatherapiairflowtestinglearning1/weather-api/{dt_string}.csv", index=False, storage_options=aws_credentials)
    print("file is done upload")
    
def load_weather():
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='current_weather_data.csv'
    )
def save_joined_data_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="task_join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature_celcius', 'feels_like_celcius', 'minimun_temp_celcius', 'maximum_temp_celcius', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'province', 'census_2024', 'land_area_sq_mile_2024'])
    now = datetime.now()
    conn = BaseHook.get_connection('s3_conn_weather')
    aws_credentials = json.loads(conn.extra)
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string
    df.to_csv(f"s3://weatherapiairflowtestinglearning1/joined-data/{dt_string}.csv", index=False,storage_options=aws_credentials)
with DAG('weather_dag_2', 
        default_args=default_args, 
        schedule='0 0 * * *',
        catchup=False) as dag:

        start_pipeline = EmptyOperator(
            task_id = 'tsk_start_pipeline'
        )
        join_data = SQLExecuteQueryOperator(
            task_id='task_join_data',
            conn_id = "postgres_conn",
            sql= '''SELECT 
                    w.city,                    
                    w.description,
                    w.temperature_celcius,
                    w.feels_like_celcius,
                    w.minimun_temp_celcius,
                    w.maximum_temp_celcius,
                    w.pressure,
                    w.humidity,
                    w.wind_speed,
                    w.time_of_record,
                    w.sunrise_local_time,
                    w.sunset_local_time,
                    c.province,
                    c.census_2024,
                    c.land_area_sq_kilo_meter_2024                    
                    FROM weather_data w
                    INNER JOIN city_look_up c
                    ON w.city = c.city                                      
                ;
                '''
            )

        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_s3
            )


        end_pipeline = EmptyOperator(
            task_id = 'tsk_end_pipeline'
        )

        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_weatherapi") as group_A:
            create_table_1 = SQLExecuteQueryOperator(
                task_id='tsk_create_table_1',
                conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS city_look_up (
                        city TEXT NOT NULL,
                        province TEXT NOT NULL,
                        census_2024 numeric NOT NULL,
                        land_area_sq_kilo_meter_2024 numeric NOT NULL                    
                    );
                '''
            )

            truncate_table = SQLExecuteQueryOperator(
                task_id='tsk_truncate_table',
                conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE city_look_up;
                    '''
            )

            uploadS3_to_postgres  = SQLExecuteQueryOperator(
                task_id = "tsk_uploadS3_to_postgres",
                conn_id = "postgres_conn",
                sql= """
                        -- Perform the import
                        SELECT aws_s3.table_import_from_s3(
                        'city_look_up', 
                        ''	, 
                        '(format csv, DELIMITER '','', HEADER true)', 
                        'sourcedatacityjava123', 
                        'the_capitals_on_the_Java.csv', 
                        'ap-southeast-2'
                        );
                """
            )

            create_table_2 = SQLExecuteQueryOperator(
                task_id='tsk_create_table_2',
                conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_celcius NUMERIC,
                    feels_like_celcius NUMERIC,
                    minimun_temp_celcius NUMERIC,
                    maximum_temp_celcius NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
            )

            is_weather_api_ready = HttpSensor(
                task_id='is_weather_api_ready',
                http_conn_id='weather_api',
                endpoint='/data/2.5/weather?q=bandung,indonesia&appid=2f1054d805456f1ff0fee1fd42a96e18'
            )

            extract_weather_data = HttpOperator(
                task_id='extract_weather_data',
                http_conn_id='weather_api',
                endpoint='/data/2.5/weather?q=bandung,indonesia&appid=2f1054d805456f1ff0fee1fd42a96e18',
                method='GET',
                response_filter=lambda response: json.loads(response.text),
                log_response=True
            )

            transform_weather_data = PythonOperator(
                task_id='transform_weather_data',
                python_callable=transform_load_data
            )

            load_weather_data = PythonOperator(
                task_id= 'tsk_load_weather_data',
                python_callable=load_weather
            )

            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> is_weather_api_ready >> extract_weather_data >> transform_weather_data >> load_weather_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline
        
        
        
        





    # is_weather_api_ready >> extract_weather_data >> transform_weather_data
    