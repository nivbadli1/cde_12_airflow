"""
ETL Data Pipeline DAG for demonstration purposes.

This DAG simulates a real-world ETL workflow with parallel processing
but only prints messages to logs without doing actual work.

Structure:
- Start task
- Extract data from multiple sources in parallel
- Transform data from each source
- Load processed data to destination
- End task with success notification
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Define default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define Python functions for our tasks
def print_hello_world_1():
    """Simulate extracting data from source 1."""
    print("Hello World 1: Extracting customer data from MySQL database")
    return "Extracted customer data"

def print_hello_world_2():
    """Simulate extracting data from source 2."""
    print("Hello World 2: Extracting transaction data from MongoDB")
    return "Extracted transaction data"

def transform_data_1():
    """Simulate transforming data from source 1."""
    print("Transforming customer data: Cleaning and normalizing...")
    return "Transformed customer data"

def transform_data_2():
    """Simulate transforming data from source 2."""
    print("Transforming transaction data: Converting currencies and aggregating...")
    return "Transformed transaction data"

def load_data():
    """Simulate loading transformed data."""
    print("Loading processed data to data warehouse...")
    return "Data loaded successfully"

def send_notification():
    """Simulate sending a completion notification."""
    print("ETL pipeline completed successfully!")
    return "Notification sent"

# Create the DAG
with DAG(
    'etl_data_pipeline',
    default_args=default_args,
    description='A demo ETL pipeline with parallel processing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'etl', 'demo'],
) as dag:

    # Define the tasks
    start = DummyOperator(
        task_id='start_pipeline',
        dag=dag,
    )
    
    extract_data_1 = PythonOperator(
        task_id='extract_data_source_1',
        python_callable=print_hello_world_1,
        dag=dag,
    )
    
    extract_data_2 = PythonOperator(
        task_id='extract_data_source_2',
        python_callable=print_hello_world_2,
        dag=dag,
    )
    
    transform_1 = PythonOperator(
        task_id='transform_source_1_data',
        python_callable=transform_data_1,
        dag=dag,
    )
    
    transform_2 = PythonOperator(
        task_id='transform_source_2_data',
        python_callable=transform_data_2,
        dag=dag,
    )
    
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_data,
        dag=dag,
    )
    
    end = PythonOperator(
        task_id='send_completion_notification',
        python_callable=send_notification,
        dag=dag,
    )
    
    # Define the task dependencies (workflow)
    start >> [extract_data_1, extract_data_2]  # Parallel extraction from two sources
    extract_data_1 >> transform_1  # Transform source 1 data
    extract_data_2 >> transform_2  # Transform source 2 data
    [transform_1, transform_2] >> load  # Wait for all transformations to complete before loading
    load >> end  # Send notification after loading is complete