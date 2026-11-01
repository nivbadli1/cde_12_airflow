"""
Slide 36 - Three ways to write the SAME DAG.   Style 1 of 3: CONTEXT MANAGER

All three files (methods_1, methods_2, methods_3) build exactly the same DAG:

        extract  >>  load

    extract returns [10, 20, 30]
    load    prints the sum  ->  60

Run all three and compare them in the UI: the graph and the result are identical.
Only the way the code is WRITTEN changes.

------------------------------------------------------------------------------
WHAT IS SPECIAL IN THIS STYLE (compare with methods_2 and methods_3)
------------------------------------------------------------------------------
* `with DAG(...) as dag:`  - every operator created INSIDE the `with` block
  automatically belongs to this DAG. You never write `dag=dag`.
* Tasks are operators: PythonOperator(task_id=..., python_callable=...).
* The dependency arrow is written by hand with  >>
* To pass a value from one task to the next you must PULL it yourself:
  ti.xcom_pull(task_ids="extract")   (XCom is explained later in the lesson)

Where you will see it: most older DAGs and most examples on the internet.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    return [10, 20, 30]            # the returned value is saved for the next task


def load(ti):
    rows = ti.xcom_pull(task_ids="extract")   # fetch what "extract" returned
    print(sum(rows))               # the log shows: 60


# STYLE 1: the DAG is a "with" block. Everything indented below belongs to it.
with DAG(
    dag_id="methods_1_context_manager",
    start_date=datetime(2024, 1, 1),
    schedule=None,                 # manual runs only (use the play button)
    catchup=False,
    tags=["methods"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> load_task      # the arrow: extract runs first, then load
