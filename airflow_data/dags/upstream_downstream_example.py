from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating the DAG
with DAG(
    'upstream_downstream_example',
    default_args=default_args,
    description='Example to demonstrate upstream and downstream',
    schedule_interval='0 0 * * *',  # Runs at midnight every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Start task
    start = DummyOperator(
        task_id='start',
    )

    # Data download task
    download_data = BashOperator(
        task_id='download_data',
        bash_command='echo "Downloading data from external source..."',
    )

    # Two processing tasks running in parallel
    process_data_a = BashOperator(
        task_id='process_data_a',
        bash_command='echo "Processing dataset A..."',
    )

    process_data_b = BashOperator(
        task_id='process_data_b',
        bash_command='echo "Processing dataset B..."',
    )

    # Task to combine processing results
    combine_results = BashOperator(
        task_id='combine_results',
        bash_command='echo "Combining processing results..."',
    )

    # Task to upload processed data
    upload_results = BashOperator(
        task_id='upload_results',
        bash_command='echo "Uploading results to data warehouse..."',
    )

    # End task
    end = DummyOperator(
        task_id='end',
    )

    # Defining relationships between tasks - Method 1 (traditional)
    start.set_downstream(download_data)
    download_data.set_downstream([process_data_a, process_data_b])  # Split for parallelism
    process_data_a.set_downstream(combine_results)
    process_data_b.set_downstream(combine_results)
    combine_results.set_downstream(upload_results)
    upload_results.set_downstream(end)

    # Alternatively, we can use the arrow syntax - Method 2 (modern)
    # start >> download_data >> [process_data_a, process_data_b] >> combine_results >> upload_results >> end