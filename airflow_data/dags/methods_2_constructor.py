"""
Slide 36 - Three ways to write the SAME DAG.   Style 2 of 3: CONSTRUCTOR

Same DAG as methods_1 and methods_3:   extract  >>  load   (load prints 60)

------------------------------------------------------------------------------
WHAT CHANGES COMPARED WITH STYLE 1 (methods_1_context_manager)
------------------------------------------------------------------------------
* No `with` block. The DAG is a normal Python variable:  dag = DAG(...)
* Because nothing is "inside" the DAG any more, EVERY operator must be told
  which DAG it belongs to:  dag=dag
  Forget it on one task and that task is not part of your DAG (a classic bug).
* The functions, the operators and the >> arrow are the same as in style 1.

Pros: explicit, you always see which DAG a task belongs to.
Cons: verbose (dag=dag repeated on every task).

Where you will see it: older code, and code that creates DAGs in a loop or from a function.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    return [10, 20, 30]


def load(ti):
    rows = ti.xcom_pull(task_ids="extract")
    print(sum(rows))               # the log shows: 60


# STYLE 2: create the DAG object first ...
dag = DAG(
    dag_id="methods_2_constructor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["methods"],
)

# ... then hand it to every task (dag=dag).  <-- this is the only difference from style 1
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)

extract_task >> load_task
