from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import urllib.request
import pandas as pd
from datetime import datetime
import json


def retrieve_data_from_bix_api(id):
    ''' Retrieves data from the BIX API and returns a SimpleHttpOperator instance.
    :param: id: Employee ID
    '''

    task_id = f'task_get_employee_id_{id}'
    http_task = SimpleHttpOperator(
        task_id=task_id,
        http_conn_id="bix_api",
        endpoint="https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior",
        method="GET",
        headers={},
        data={"id": id},
    )

    return http_task


import requests
import pandas as pd


def download_parquet_file():
    ''' Downloads the parquet file from the Google Cloud Storage and returns it as a DataFrame. '''

    url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if download fails

    with open("categoria.parquet", "wb") as file:
        file.write(response.content)
        print("Parquet file downloaded successfully!")

    df = pd.read_parquet("categoria.parquet")
    return df


def transform_data_from_bix_api(api_data):
    ''' Receives data from the BIX API and transforms it into a Pandas DataFrame. '''

    received_data = {
        'api_data': [api_data],
    }
    df = pd.DataFrame(received_data)
    return df


def transform_data_from_postgres(**context):
    ''' Receives data from the PostgreSQL database and transforms it into a Pandas DataFrame. The data is received as
     a list of lists, where each list is a row from the database.
     :param context: Task context
     '''

    postgres_data = context['ti'].xcom_pull(task_ids='task_retrieve_data_from_postgresql')
    context['ti'].xcom_push(key='postgres_data', value=postgres_data)


def generate_parquet_file(**context):
    ''' Generates a parquet file from a Pandas DataFrame.
     :param context: Task context
     '''
    postgres_data = context['ti'].xcom_pull(key='postgres_data', task_ids='task_transform_data_from_postgres')
    df = transform_data_from_postgres(postgres_data)
    file_name = 'postgres_data.parquet'
    df.to_parquet(file_name)
    print(f"Parquet file '{file_name}' generated successfully!")


with DAG('test_pipeline_dag', description='Pipeline DAG to retrieve data from the BIX API, PostgreSQL database, and a parquet file.',
         schedule_interval='@daily', start_date=datetime(2023, 6, 16), catchup=False) as dag:

    start_task = DummyOperator(
        task_id='task_start_pipeline'
    )

    tasks = []

    for id in range(1, 10):
        retrieve_data_task = retrieve_data_from_bix_api(id)
        tasks.append(retrieve_data_task)

    retrieve_data_task_postgres = PostgresOperator(
        task_id='task_retrieve_data_from_postgresql',
        postgres_conn_id='bix_database',
        sql='SELECT * FROM public.venda'
    )

    task_download = PythonOperator(
        task_id='task_download_parquet_file',
        python_callable=download_parquet_file
    )

    task_transform_api = PythonOperator(
        task_id='task_transform_data_from_api',
        python_callable=transform_data_from_bix_api,
        op_kwargs={'api_data': '{{ task_instance.xcom_pull(task_ids="task_get_employee_id_" + str(task_id)) }}'},
        provide_context=True,
    )

    task_transform_postgres = PythonOperator(
        task_id='task_transform_data_from_postgres',
        python_callable=transform_data_from_postgres,
        op_kwargs={'postgres_data': '{{ task_instance.xcom_pull(task_ids="task_retrieve_data_from_postgresql") }}'},
    )

    task_generate_parquet_postgres = PythonOperator(
        task_id='task_generate_parquet_file_postgres',
        python_callable=generate_parquet_file,
        provide_context=True,
    )

    start_task >> tasks[0:] >> task_transform_api
    start_task >> task_download
    start_task >> retrieve_data_task_postgres >> task_transform_postgres >> task_generate_parquet_postgres


