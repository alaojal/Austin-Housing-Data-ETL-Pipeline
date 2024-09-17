# import neccessary libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import pandas as pd 
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook




# Temperature conversion function
def kelvin_to_fahrenheit(temp_in_kelvin):
        temp_fahrenheit = round((temp_in_kelvin - 273.15) * (9/5) + 32, 4)
        return temp_fahrenheit

# Data transformation function
def transform_data(task_instance):
    extract_data = task_instance.xcom_pull(task_ids = 'group_a.tsk_extract_data')
    
    # variables for the data set
    city = extract_data['name']
    latitude_position = extract_data['coord']['lat']
    longitude_position = extract_data['coord']['lon']
    weather_description = extract_data['weather'][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(extract_data['main']['temp'])
    feel_like_fahrenheit = kelvin_to_fahrenheit(extract_data['main']['feels_like'])
    min_temp_fahrenheit = kelvin_to_fahrenheit(extract_data['main']['temp_min'])
    max_temp_fahrenheit = kelvin_to_fahrenheit(extract_data['main']['temp_max'])
    pressure = extract_data['main']['pressure']
    humidity = extract_data['main']['humidity']
    wind_speed = extract_data['wind']['speed']
    time_of_record = datetime.fromtimestamp(extract_data['dt'] + extract_data['timezone'])
    sunrise_time = datetime.fromtimestamp(extract_data['sys']['sunrise'] + extract_data['timezone'])
    sunset_time = datetime.fromtimestamp(extract_data['sys']['sunset'] + extract_data['timezone'])

    # put all these variables in python dictionary
    transformed_data = {
        'City': city,
        'lat_location': latitude_position,
        'long_location': longitude_position,
        'description': weather_description,
        'temperature': temp_fahrenheit,
        'feels_like_temp': feel_like_fahrenheit,
        'minimum_temp': min_temp_fahrenheit,
        'maximum_temp': max_temp_fahrenheit,
        'pressure': pressure,
        'humidity': humidity,
        'wind_speed': wind_speed,
        'time_of_record': time_of_record,
        'sunrise_local_time': sunrise_time,
        'sunset_local_time': sunset_time
    }
    #print(transformed_data)

    # put the transformed data dictionary in a list
    transformed_data_list = [transformed_data]

    # convert to a dataFrame
    df_data = pd.DataFrame(transformed_data_list)

    # save to csv
    df_data.to_csv(f"current_houston_weather_data.csv", index = False, header = False)
    
    
def load_transformed_data_to_postgres():
    hook = PostgresHook(postgres_conn_id = 'postgres_conn')
    hook.copy_expert(
        sql = "COPY houston_weather_data FROM stdin WITH DELIMITER as ','",
        filename = 'current_houston_weather_data.csv'             
        )

# load data into s3 bucket function
def save_joined_data(task_instance):
    data = task_instance.xcom_pull(task_ids = 'tsk_join_data')
    df = pd.DataFrame(data, columns = ['City','lat_location','long_location','description',
        'temperature','feels_Like_temp','minimum_temp','maximum_temp','pressure','humidity',
        'wind_speed','time_of_record','sunrise_local_time','sunset_local_time','State',
        'census_2020','land_area_sq_mile_2020'])
    #df.to_csv('joined_houston_weather_data.csv', index = False)
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    dt_string = 'join_houston_weather_data_' + dt_string
    #df.to_csv(f"{dt_string}.csv", index = False)
    df.to_csv(f"s3://s3bucketforweathermapdata/{dt_string}.csv", index = False)

default_args = {
    'owner': 'weathetr-airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'email': ['alaoajala@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'entries': 2,
    'retry_delay': timedelta(minutes = 2) 
}

with DAG('weathermap_parallel_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False) as dag:

    # start pipelin
    start_pipeline = DummyOperator(
        task_id = 'tsk_start_pipeline'
    )
    # Task: End pipeline
    end_pipeline = DummyOperator(
        task_id = 'tsk_end_pipeline'
    )
    # Task: Join data
    join_data = PostgresOperator(
        task_id = 'tsk_join_data',
        postgres_conn_id = 'postgres_conn',
        sql = ''' SELECT
        h.City,
        h.lat_location, 
        h.long_location,
        h.description,
        h.temperature ,
        h.feels_Like_Temp, 
        h.minimum_Temp,
        h.maximum_Temp,
        h.pressure,
        h.humidity,
        h.wind_Speed,
        h.time_of_Record,
        h.sunrise_Local_Time,
        h.sunset_Local_Time,
        u.State,
        u.Census_2020,
        u.land_area_sq_mile_2020
        FROM houston_weather_data h
        INNER JOIN us_city_lookup u
            ON h.City = u.City;
        '''
    )

     # Task: load to S3 bucket
    load_join_data = PythonOperator(
        task_id = 'tsk_load_join_data',
        python_callable = save_joined_data
    )

   # create a task group dag
    with TaskGroup(group_id = 'group_a', tooltip = 'extract_from_s3_and_weathermap_api') as group_A:
        # create table
        create_table_a = PostgresOperator(
            task_id = 'tsk_create_table_a',
            postgres_conn_id = 'postgres_conn',
            sql = ''' 
            CREATE TABLE IF NOT EXISTS us_city_lookup(
            City VARCHAR NOT NULL,
            State VARCHAR NOT NULL,
            Census_2020 NUMERIC NOT NULL,
            land_area_sq_mile_2020 NUMERIC NOT NULL
            );'''
        )
        # Truncate table task
        truncate_table = PostgresOperator(
            task_id = 'tsk_truncate_table',
            postgres_conn_id = 'postgres_conn',
            sql = '''
            TRUNCATE TABLE us_city_lookup;
            '''
        )
        # upload table to s3 bucket
        upload_from_s3_to_postgres = PostgresOperator(
            task_id = 'tsk_upload_from_s3_to_postgres',
            postgres_conn_id = 'postgres_conn',
            sql = '''SELECT aws_s3.table_import_from_s3('us_city_lookup', '', '(format csv, DELIMITER ",", HEADER true)',
            's3bucketforweathermapdata', 'us_city.csv', 'us-east-2');'''
        )
        # create a second table
        create_table_b = PostgresOperator(
            task_id = 'tsk_create_table_b',
            postgres_conn_id = 'postgres_conn',
            sql = '''
            CREATE TABLE IF NOT EXISTS houston_weather_data(
            City VARCHAR,
            lat_location VARCHAR,
            long_location VARCHAR,
            description VARCHAR,
            temperature VARCHAR,
            feels_Like_Temp VARCHAR,
            minimum_Temp VARCHAR,
            maximum_Temp VARCHAR,
            pressure VARCHAR,
            humidity VARCHAR,
            wind_Speed VARCHAR,
            time_of_Record TIMESTAMP,
            sunrise_Local_Time TIMESTAMP,
            sunset_Local_Time TIMESTAMP
            );
            '''
        )
        # task to check if the weathermap api is ready
        is_houston_weather_api_ready = HttpSensor(
            task_id = 'tsk_is_houston_weather_api_ready',
            http_conn_id = 'weathermap_api',
            endpoint = '/data/2.5/weather?q=Houston&appid=df3fd3d570899557fdbec0607810f9f5'
        )
        # task for data extraction from weathermap API
        extarct_data = SimpleHttpOperator(
            task_id = 'tsk_extract_data',
            http_conn_id = 'weathermap_api',
            endpoint = '/data/2.5/weather?q=houston&appid=df3fd3d570899557fdbec0607810f9f5',
            method = 'GET',
            response_filter = lambda r:json.loads(r.text),
            log_response = True
           )
        # data transformation task
        data_transformation = PythonOperator(
            task_id = 'tsk_data_transformation',
            python_callable = transform_data
           )
        # create task to load transformed data into postgres
        load_weather_data_to_postgres = PythonOperator(
            task_id = 'tsk_load_weather_data_to_postgres',
            python_callable = load_transformed_data_to_postgres
        )


    # create dag dependencies in the inner dag
    create_table_a >> truncate_table >> upload_from_s3_to_postgres
    create_table_b >> is_houston_weather_api_ready >> extarct_data >> data_transformation >> load_weather_data_to_postgres
# create dedenecies between the dags
start_pipeline >> group_A >> join_data >> load_join_data >> end_pipeline

    