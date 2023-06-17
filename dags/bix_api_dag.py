from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def retrieve_data_from_bix_api(id):
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

def process_data(ti):
    api_responses = []
    task_ids = [f'task_get_employee_id_{id}' for id in range(1, 10)]
    for task_id in task_ids:
        response = ti.xcom_pull(task_ids=task_id)
        api_responses.append(response)
    # Process the API responses stored in the list 'api_responses'

with DAG('bix_api_dag', description='DAG to retrieve data from the BIX API.',
              schedule_interval='@daily', start_date=datetime(2023, 6, 15), catchup=False) as dag:
    tasks = []

    for id in range(1, 10):
        retrieve_data_task = retrieve_data_from_bix_api(id)
        tasks.append(retrieve_data_task)

    start_task = DummyOperator(
        task_id='task_start_pipeline'
    )

    end_task = PythonOperator(
        task_id='task_process_data',
        python_callable=process_data
    )

    start_task >> tasks[0:] >> end_task
