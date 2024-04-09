from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'task_states_demo',
    default_args=default_args,
    description='Demonstration of task states using Bash Operator',
    schedule_interval=timedelta(days=1)
)

# Task 1: Successful task
successful_task = BashOperator(
    task_id='successful_task',
    bash_command='echo "This is a successful task"',
    dag=dag
)

# Task 2: Failed task
failed_task = BashOperator(
    task_id='failed_task',
    bash_command='exit 1',
    dag=dag
)

# Task 3: Skipped task
skipped_task = BashOperator(
    task_id='skipped_task',
    bash_command='echo "This task is skipped"',
    dag=dag,
    trigger_rule='none_failed_or_skipped'
)

# Task 4: Retried task
retried_task = BashOperator(
    task_id='retried_task',
    bash_command='exit 1',
    dag=dag
)

# Task dependencies
successful_task >> failed_task >> skipped_task
successful_task >> retried_task