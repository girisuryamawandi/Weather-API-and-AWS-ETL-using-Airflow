from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
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
    data = task_instance.xcom_pull(task_ids="extract_weather_data") 
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
                        "Sunset (Local Time)": sunset_time,
                        "conn_key": conn.extra                 
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    print(conn)
    aws_credentials = json.loads(conn.extra)
    now = datetime.now()    
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_bandung_' + dt_string
    # df_data.to_csv(f"/opt/airflow/dags/{dt_string}.csv",index=False)
    
    df_data.to_csv(f"s3://weatherapiairflowtestinglearning1/weather-api/{dt_string}.csv", index=False, storage_options=aws_credentials)
    print("file is done upload")


with DAG('weather_dag', 
        default_args=default_args, 
        schedule='0 0 * * *',
        catchup=False) as dag:

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

    is_weather_api_ready >> extract_weather_data >> transform_weather_data
    