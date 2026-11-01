"""
Slide 36 - Three ways to write the SAME DAG.   Style 3 of 3: DECORATORS (TaskFlow API)

Same DAG as methods_1 and methods_2:   extract  >>  load   (load prints 60)

------------------------------------------------------------------------------
WHAT CHANGES COMPARED WITH STYLES 1 AND 2
------------------------------------------------------------------------------
* @dag turns a Python function into the DAG.   @task turns a function into a task.
  No PythonOperator, no python_callable, no task_id (the function name is the task id).
* No `>>` arrow. Calling  load(extract())  creates the dependency:
  Airflow sees that load needs the output of extract, so extract runs first.
* No xcom_pull. `return [10, 20, 30]` is saved automatically, and
  load(rows) receives it as a normal function argument.
* You must CALL the DAG function at the bottom of the file (etl()),
  otherwise Airflow never creates the DAG.

Pros: shortest and easiest to read, data passing is automatic.
Cons: it hides some of the machinery, so you still need to understand styles 1 and 2
      to read older code.

This is the style we use in the lab.
"""
from datetime import datetime

from airflow.decorators import dag, task


# STYLE 3: the decorator turns the function below into a DAG.
@dag(
    dag_id="methods_3_decorators",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["methods"],
)
def etl():

    @task
    def extract():
        return [10, 20, 30]        # saved automatically for the next task

    @task
    def load(rows):                # "rows" is filled in automatically
        print(sum(rows))           # the log shows: 60

    load(extract())                # this one line is the arrow: extract >> load


etl()                              # do not forget this call!
