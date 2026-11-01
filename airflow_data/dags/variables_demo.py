"""
Airflow Variables demo - simple version.

Create this Variable in the UI before running (Admin -> Variables -> +):
  key = environment    value = dev
"""
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def read_variable():
    # Read a Variable (default_var is used if it does not exist)
    env = Variable.get("environment", default_var="dev")
    print(f"environment = {env}")


def write_variable():
    # Write a Variable from code
    Variable.set("last_run_at", datetime.now().isoformat())
    print("Saved Variable last_run_at")


with DAG(
    dag_id="variables_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
) as dag:

    read_task = PythonOperator(
        task_id="read_variable",
        python_callable=read_variable,
    )

    write_task = PythonOperator(
        task_id="write_variable",
        python_callable=write_variable,
    )

    read_task >> write_task
