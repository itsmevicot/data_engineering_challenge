from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime


dag = DAG('postgres_dag',
    start_date=datetime(2023, 6, 15),
    description='DAG to retrieve data from the PostgreSQL database provided by BIX to the Junior Data Engineering Challenge.',
    schedule_interval='@daily',
    catchup=False
)

retrieve_data_task = PostgresOperator(
    task_id='task_retrieve_data_from_postgresql',
    postgres_conn_id='bix_database',
    sql='SELECT * FROM public.venda',
    dag=dag
)
