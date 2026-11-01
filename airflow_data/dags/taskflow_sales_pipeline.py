"""
Slide 34 - TaskFlow API (the code from the slide, with the imports it needs).
dag_id = sales_pipeline, the same name the CLI slide (slide 38) uses:
    airflow dags trigger sales_pipeline
    airflow tasks test sales_pipeline extract 2024-01-01
"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(schedule="@daily",
     start_date=datetime(2024, 1, 1),
     catchup=False)
def sales_pipeline():

    @task
    def extract():
        return [10, 20, 30]

    @task
    def load(rows):
        print(sum(rows))      # the log of "load" shows: 60

    load(extract())


sales_pipeline()
