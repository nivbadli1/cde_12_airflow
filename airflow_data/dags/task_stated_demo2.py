"""
Task States Demonstration DAG

This DAG demonstrates the different task states in Airflow:
- success: Task executed successfully
- failed: Task failed and exceeded retry attempts
- up_for_retry: Task failed but will be retried
- skipped: Task was skipped due to branching or trigger rules
- upstream_failed: Task was not executed because an upstream task failed

The DAG includes examples of each state and how they interact.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import random

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,                   # Allow one retry for failed tasks
    'retry_delay': timedelta(seconds=30)  # Wait 1 minute before retrying
}

# DAG definition
dag = DAG(
    'task_states_demo_complete',
    default_args=default_args,
    description='Complete demonstration of all Airflow task states',
    catchup=False,  # Don't backfill for missed schedule intervals
    schedule_interval=timedelta(days=1)  # Run daily
)

# -------------- TASK DEFINITIONS --------------

# Start dummy task - serves as a clear entry point for the DAG
start = DummyOperator(
    task_id='start',
    dag=dag
)

# 1. Task that will always succeed
successful_task = BashOperator(
    task_id='successful_task',
    bash_command='echo "This task will succeed"',
    dag=dag
)

# 2. Task that will always fail (even after retry)
# This will show as "failed" in the UI after all retries are exhausted
failed_task = BashOperator(
    task_id='failed_task',
    bash_command='exit 1',  # Exit code 1 causes the task to fail
    dag=dag
)

# 3. Python task that will fail on first attempt but succeed on retry
# This demonstrates the "up_for_retry" and then "success" states
def maybe_fail(**context):
    attempt_number = context['ti'].try_number
    if attempt_number == 1:
        # First attempt will fail
        raise Exception("First attempt fails, retry will succeed")
    # Second attempt will succeed
    return "Success on retry!"

retried_task = PythonOperator(
    task_id='retried_task',
    python_callable=maybe_fail,
    dag=dag
)

# 4. Bash task that will fail on first attempt but succeed on retry
# This is similar to retried_task but uses BashOperator instead
bash_retry_task = BashOperator(
    task_id='bash_retry_task',
    # This bash command checks the try_number and fails on first attempt only
    bash_command='if [ "{{ task_instance.try_number }}" == "1" ]; then exit 1; else echo "Success on retry!"; fi',
    dag=dag
)

# 5. Task that will be skipped because upstream task failed
# This demonstrates the "upstream_failed" state
skipped_task = BashOperator(
    task_id='skipped_task',
    bash_command='echo "This task will be skipped"',
    # ALL_SUCCESS means this task only runs if all upstream tasks succeeded
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# 6. Another task that will be skipped due to upstream failure
# This also demonstrates the "upstream_failed" state
upstream_failed_task = BashOperator(
    task_id='upstream_failed_task',
    bash_command='echo "This would run but upstream failed"',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# 7. Task that runs even if upstream tasks fail
# This demonstrates how to override the default behavior
always_run_task = BashOperator(
    task_id='always_run_task',
    bash_command='echo "This task always runs"',
    # ALL_DONE means this task runs regardless of upstream task states
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# 8. Task that will run after a successful retry
# This demonstrates successful dependency after a retry
after_retry_task = BashOperator(
    task_id='after_retry_task',
    bash_command='echo "This task runs after a successful retry"',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)


# End dummy task - serves as a clear exit point for the DAG
# Uses ALL_DONE so it always runs at the end of the workflow
end = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# -------------- TASK DEPENDENCIES --------------

# Define the workflow relationships between tasks using >> operator

# Path 1: Start -> Successful Task -> Retried Task (Python) -> After Retry Task -> End
# (After Retry Task will run because Retried Task eventually succeeds)
start >> successful_task >> retried_task >> after_retry_task >> end

# Path 2: Start -> Bash Retry Task -> End
start >> bash_retry_task >> end

# Path 3: Start -> Failed Task -> Skipped Task -> End
# (Skipped Task will show as "upstream_failed")
start >> failed_task >> skipped_task >> end

# Path 4: Failed Task -> Upstream Failed Task -> End
# (Upstream Failed Task will show as "upstream_failed")
failed_task >> upstream_failed_task >> end

# Path 5: Failed Task -> Always Run Task -> End
# (Always Run Task will execute despite Failed Task failing)
failed_task >> always_run_task >> end