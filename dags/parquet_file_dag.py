import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import urllib.request
from datetime import datetime


def download_parquet_file():
    url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
    file_name = "categoria.parquet"
    urllib.request.urlretrieve(url, file_name)
    print("Parquet file downloaded successfully!")


def read_and_store_data():
    parquet_file = "categoria.parquet"
    df = pd.read_parquet(parquet_file)
    print(df.head())


dag = DAG('parquet_file_dag', description='DAG to download and store a parquet file.',
          schedule_interval='@daily', start_date=datetime(2023, 6, 15), catchup=False)

task_download = PythonOperator(task_id='download_parquet_file',
                               python_callable=download_parquet_file,
                               dag=dag)

task_read_store = PythonOperator(task_id='read_and_store_data',
                                 python_callable=read_and_store_data,
                                 dag=dag)

task_download >> task_read_store
