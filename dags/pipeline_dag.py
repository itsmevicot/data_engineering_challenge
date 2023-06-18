from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import os
from datetime import datetime


def extract_data_from_bix_api(id):
    ''' Retrieves data from the BIX API and returns a SimpleHttpOperator instance.
    :param: id: Employee ID
    '''

    http_task = SimpleHttpOperator(
        task_id=f'task_get_employee_id_{id}',
        http_conn_id="bix_api",
        endpoint="https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior",
        method="GET",
        data={"id": id},
    )
    return http_task


def extract_parquet_file_from_gcs():
    '''Downloads the parquet file from the Google Cloud Storage and returns it as a DataFrame.'''

    url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
    response = requests.get(url)
    response.raise_for_status()


    with open('tmp/categoria.parquet', "wb") as file:
        file.write(response.content)
        print("Parquet file downloaded successfully!")

    df = pd.read_parquet('tmp/categoria.parquet')
    return df


def transform_data_from_api(ti):
    ''' Receives data from the BIX API and transforms it into a Parquet file.
     :param ti: Task Instance '''

    data_list = []
    for id in range(1, 10):
        employee = ti.xcom_pull(task_ids=f'task_get_employee_id_{id}')
        data_dict = {
            'id': str(id),
            'nome_funcionario': str(employee)
        }
        data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    df.to_parquet('tmp/funcionarios.parquet', index=None)
    df = pd.read_parquet('tmp/funcionarios.parquet')

    return df


def task_transform_data_from_postgresql(ti):
    data_list = []
    linhas = ti.xcom_pull(task_ids='task_extract_data_from_postgresql')
    for linha in linhas:
        data_dict = {
            'id_venda': linha[0],
            'id_funcionario': linha[1],
            'id_categoria': linha[2],
            'data_venda': linha[3],
            'venda': linha[4]
        }
        data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    df.to_parquet('tmp/vendas.parquet', index=None)
    df = pd.read_parquet('tmp/vendas.parquet')

    return df


def cleanup_temp_files():
    count = 0
    for filename in os.listdir('tmp'):
        file_path = os.path.join('tmp', filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            count += 1
            print(f"Deleted file: {file_path}")
    print(f"Deleted {count} files.")



with DAG('pipeline_dag', description='Pipeline DAG to retrieve data from the BIX API, PostgreSQL database, and a parquet file.',
         schedule_interval='@daily', start_date=datetime(2023, 6, 16), catchup=False) as dag:

    start_task = DummyOperator(
        task_id='task_start_pipeline'
    )

    tasks = []

    for id in range(1, 10):
        retrieve_data_task = extract_data_from_bix_api(id)
        tasks.append(retrieve_data_task)


    task_extract_data_from_postgresql = PostgresOperator(
        task_id='task_extract_data_from_postgresql',
        postgres_conn_id='bix_database',
        sql='SELECT * FROM public.venda;'
    )

    task_extract_parquet_file_from_gcs = PythonOperator(
        task_id='task_extract_parquet_file_from_gcs',
        python_callable=extract_parquet_file_from_gcs
    )

    task_transform_data_from_api = PythonOperator(
        task_id='task_transform_data_from_api',
        python_callable = transform_data_from_api
    )

    task_transform_data_from_postgresql = PythonOperator(
        task_id='task_transform_data_from_postgresql',
        python_callable = task_transform_data_from_postgresql
    )

    task_load_data_from_categories_file = PostgresOperator(
        task_id='task_load_data_from_categories_file',
        postgres_conn_id='local_database',
        sql='sql/insert_data_into_categorias.sql',
    )

    task_load_data_from_employees_file = PostgresOperator(
        task_id='task_load_data_from_employees_file',
        postgres_conn_id='local_database',
        sql='sql/insert_data_into_funcionarios.sql',
    )

    task_load_data_from_postgresql = PostgresOperator(
        task_id='task_load_data_from_postgresql',
        postgres_conn_id='local_database',
        sql='sql/insert_data_into_vendas.sql',
    )

    task_to_cleanup = PythonOperator(
        task_id="task_to_cleanup",
        python_callable=cleanup_temp_files
    )


    start_task >> tasks[0:] >> task_transform_data_from_api >> task_load_data_from_employees_file
    start_task >> task_extract_parquet_file_from_gcs >> task_load_data_from_categories_file
    start_task >> task_extract_data_from_postgresql >> task_transform_data_from_postgresql

    task_load_data_from_categories_file >> task_load_data_from_postgresql
    task_load_data_from_employees_file >> task_load_data_from_postgresql
    task_transform_data_from_postgresql >> task_load_data_from_postgresql

    task_load_data_from_postgresql >> task_to_cleanup