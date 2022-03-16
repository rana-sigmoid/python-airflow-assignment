from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from utils import get_weather_api_method

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
with DAG("Weather_Dag", default_args=default_args, schedule_interval='* 6 * * *',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:
    # Filling up the CSV with the 10 states weather data
    task1 = PythonOperator(task_id="check_file_exist_or_create_new_file", python_callable=get_weather_api_method)
    # Creating the table same as csv columns
    task2 = PostgresOperator(task_id="create_new_table", postgres_conn_id='postgres_conn', sql="create_new_table.sql")
    # Filling up the columns of the table while reading the data from the csv file
    task3 = PostgresOperator(task_id="insert_data_into_table", postgres_conn_id='postgres_conn',
                          sql="copy weather FROM '/store_files_postgresql/weather_data.csv' DELIMITER ',' CSV HEADER ;")
    task1 >> task2 >> task3
