"""
Nightly report: the DAG for Exercise 4 (operate Airflow from a Python script).

    extract  >>  transform  >>  load

The DAG never runs by itself (schedule=None): a script triggers it.
You can make it FAIL on purpose, without touching the code, with a Variable:

    airflow variables set fail_mode on     # transform raises an error
    airflow variables set fail_mode off    # everything succeeds (also the default)
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="nightly_report",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise"],
)
def nightly_report():

    @task
    def extract():
        rows = [120, 80, 95, 143, 60]
        print(f"extract: read {len(rows)} rows")
        return rows

    @task
    def transform(rows):
        fail_mode = Variable.get("fail_mode", default_var="off")
        print(f"transform: fail_mode = {fail_mode}")
        if fail_mode == "on":
            raise ValueError("transform failed on purpose (fail_mode = on)")
        return sum(rows)

    @task
    def load(total):
        print(f"load: the nightly total is {total}")

    load(transform(extract()))


nightly_report()
