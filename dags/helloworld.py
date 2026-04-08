from airflow import DAG
try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello World from Airflow!'

with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule='@daily',
    tags=['example']
) as dag:
    
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )

    hello_task
