"""
Simple DAG with just start, hello world, and end tasks.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to print Hello World
def print_hello():
    print("Hello World")
    return "Hello World message logged"

# Create the DAG
with DAG(
    'simple_hello_world',
    default_args=default_args,
    description='A simple DAG with start, hello world, and end tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['team_date', 'sales',"s3"],
) as dag:

    # Define the tasks
    start = DummyOperator(
        task_id='start',
    )
    
    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=print_hello,
    )
    
    end = DummyOperator(
        task_id='end',
    )
    
    # Set task dependencies
    start >> hello_world >> end