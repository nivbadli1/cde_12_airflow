# CLI Practice: Solution

Commands are shown as typed **inside** the container (`docker compose exec airflow-scheduler bash`).
The output was captured on Airflow 2.8.4. Your dates, run ids and timings will be different.

---

## Part 1: Look around

```bash
airflow version
```
```
2.8.4
```

```bash
airflow dags list | grep -E "hello|upstream|sales|states|variables|etl"
```
```
etl_data_pipeline           | etl_data_pipeline.py           | data_engineering | None
sales_pipeline              | taskflow_sales_pipeline.py     | airflow          | None
simple_hello_world          | hello_world.py                 | airflow          | None
task_states_demo            | task_states_demo.py            | airflow          | None
task_states_demo_complete   | task_stated_demo2.py           | airflow          | None
upstream_downstream_example | upstream_downstream_example.py | data_team        | None
variables_demo              | variables_demo.py              | airflow          | None
```
(In your stack the last column, `paused`, shows `True` or `False`.)

```bash
airflow dags list-import-errors
```
```
No data found
```
`No data found` is the **good** answer: no file has an import error. If a DAG is missing from the list, this is the first command to run.

```bash
airflow tasks list upstream_downstream_example --tree
```
```
<Task(EmptyOperator): start>
    <Task(BashOperator): download_data>
        <Task(BashOperator): process_data_a>
            <Task(BashOperator): combine_results>
                <Task(BashOperator): upload_results>
                    <Task(EmptyOperator): end>
        <Task(BashOperator): process_data_b>
            <Task(BashOperator): combine_results>
                <Task(BashOperator): upload_results>
                    <Task(EmptyOperator): end>
```
**Answers:** `process_data_a` and `process_data_b` can run in parallel (they are both children of `download_data`).
There are 6 steps in a row on the longest path: `start`, `download_data`, a processing task, `combine_results`, `upload_results`, `end`.
The tree prints `combine_results` twice only because it has two parents; it is one task.

---

## Part 2: Run a DAG from the CLI

```bash
airflow dags unpause simple_hello_world
```
```
Dag: simple_hello_world, paused: False
```

```bash
airflow dags trigger simple_hello_world
```
The printed table ends with `state = queued`: the run was created, and the scheduler picks it up a moment later.

```bash
airflow dags list-runs -d simple_hello_world
```
```
dag_id             | run_id                               | state   | execution_date            | ...
===================+======================================+=========+===========================+====
simple_hello_world | manual__2026-11-01T14:42:52+00:00    | success | 2026-11-01T14:42:52+00:00 | ...
simple_hello_world | scheduled__2026-10-31T00:00:00+00:00 | success | 2026-10-31T00:00:00+00:00 | ...
```
Run the command again if your run is still `queued` or `running`.

```bash
airflow tasks states-for-dag-run simple_hello_world "manual__2026-11-01T14:42:52+00:00"
```
(Copy the `run_id` from the previous output and keep the quotes.)
```
dag_id             | execution_date            | task_id     | state   | ...
===================+===========================+=============+=========+====
simple_hello_world | 2026-11-01T14:42:52+00:00 | start       | success | ...
simple_hello_world | 2026-11-01T14:42:52+00:00 | hello_world | success | ...
simple_hello_world | 2026-11-01T14:42:52+00:00 | end         | success | ...
```

**Answer, why two runs?** The `manual__...` run is yours (from `trigger`).
The `scheduled__...` run was created by the scheduler right after you unpaused the DAG: the DAG has `catchup=False`, so the scheduler runs only the **most recent** daily interval, once, instead of every missed day since 2023.

---

## Part 3: Test a single task

```bash
airflow tasks test sales_pipeline extract 2024-01-01
```
The log ends with:
```
INFO - Done. Returned value was: [10, 20, 30]
```

```bash
airflow dags list-runs -d sales_pipeline
```
```
No data found
```
**No run was created.** `tasks test` does not create a DAG run and does not record a task state, which is why it is safe to try as often as you like.

```bash
airflow tasks test task_states_demo_complete failed_task 2024-01-01
echo $?
```
The log ends with a traceback and this line, and the exit code is non-zero:
```
airflow.exceptions.AirflowException: Bash command failed. The command returned a non-zero exit code 1.
1
```
A non-zero exit code is how scripts and CI systems know that something failed.

```bash
airflow tasks test sales_pipeline load 2024-01-01
```
It **fails**:
```
TypeError: 'NoneType' object is not iterable
```
`tasks test` runs one task on its own. `load` expects the list that `extract` returned, but nothing was recorded for it, so it receives `None` and `sum(None)` crashes.
Tasks that depend on the output of an earlier task must be tested together:

```bash
airflow dags test sales_pipeline
```
The whole DAG runs in one process and the log of `load` prints:
```
60
```

**Why is `tasks test` the safest way to try a new task?** It runs one task, shows the log in your terminal, and records no DAG run and no task state, so a mistake leaves no red squares in the UI.
(One caution: whatever your task code does itself, such as writing a file or setting a Variable, still happens.)

---

## Part 4: Variables from the CLI

```bash
airflow variables set environment dev
airflow variables get environment
airflow variables list
```
```
Variable environment created
dev
key
===========
environment
```

```bash
airflow tasks test variables_demo read_variable 2024-01-01
```
```
environment = dev
```

```bash
airflow variables set environment prod
airflow tasks test variables_demo read_variable 2024-01-01
```
```
environment = prod
```
The DAG file did not change: only the Variable did.

```bash
airflow variables delete environment
airflow tasks test variables_demo read_variable 2024-01-01
```
```
Variable environment deleted
environment = dev
```
After deleting, the code falls back to its default: `Variable.get("environment", default_var="dev")`. This is why `default_var` matters: without it the task would fail.

---

## Part 5: Challenge

1. Unpause, trigger, and show only the successful runs:
   ```bash
   airflow dags unpause etl_data_pipeline
   airflow dags trigger etl_data_pipeline
   airflow dags list-runs -d etl_data_pipeline --state success -o plain
   ```
   (`-o plain` prints a compact table; the run takes about 20 seconds to finish.)
2. The runs as JSON:
   ```bash
   airflow dags list-runs -d etl_data_pipeline -o json
   ```
3. Many commands accept `-o`, for example:
   ```bash
   airflow dags list -o plain
   ```
   Other formats are `table` (the default), `json` and `yaml`.
4. Pause what you unpaused:
   ```bash
   airflow dags pause simple_hello_world
   airflow dags pause etl_data_pipeline
   ```
   ```
   Dag: simple_hello_world, paused: True
   Dag: etl_data_pipeline, paused: True
   ```
