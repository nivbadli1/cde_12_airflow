# CLI Practice: Run Airflow from the Terminal

**Time:** about 20 minutes. **Goal:** do from the terminal what you already do in the UI: list DAGs, run them, check their state, test a single task and set Variables.

**Before you start**

1. The stack is running (Lab Step 1) and you pulled the latest repo (`git pull`), so these DAG files exist in `airflow_data/dags/`:
   `simple_hello_world`, `upstream_downstream_example`, `sales_pipeline`, `task_states_demo_complete`, `variables_demo`, `etl_data_pipeline`.
2. Wait about 30 seconds after pulling, then check that they appear in the UI.

## How to run the commands

Airflow lives inside the containers, so every command runs inside one of them. Two options:

```bash
# Option A: one command at a time, from your project folder
docker compose exec airflow-scheduler airflow version

# Option B: open a shell inside the container once, then type the airflow commands directly
docker compose exec airflow-scheduler bash
airflow version
```

The commands below are written the Option B way (starting with `airflow`).
Lines that mention `RemovedInAirflow3Warning` are harmless warnings, ignore them.

---

## Part 1: Look around (read-only)

1. Print the Airflow version.
2. List all DAGs. Find the six DAGs above (tip: add `| grep -E "hello|upstream|sales|states|variables|etl"` to filter).
3. Check whether any DAG file has an import error.
4. Show the tasks of `upstream_downstream_example` **as a tree**.

**Answer in your notebook:** which two tasks can run in parallel? How many steps are there from `start` to `end`?

## Part 2: Run a DAG from the CLI

1. Unpause `simple_hello_world`.
2. Trigger it. Look at the table that is printed: what is the `state`?
3. List the runs of this DAG and wait until your run is `success`.
4. Copy your run id from the list and print the **state of every task** in that run.
5. Open the Grid view in the UI and check that it shows the same thing.

**Answer in your notebook:** you probably see **two** runs, not one. Which one did you create, and where does the other one come from?

## Part 3: Test a single task safely

`airflow tasks test <dag_id> <task_id> <date>` runs **one task, right now, in your terminal**.

1. Test the task `extract` of `sales_pipeline` with the date `2024-01-01`. What value does it return?
2. List the runs of `sales_pipeline`. Did the test create a run?
3. Test the task `failed_task` of `task_states_demo_complete`. Right after it, print the exit code of the last command (`echo $?`).
4. Now test the task `load` of `sales_pipeline`. What happens? Why?
5. Run the **whole** `sales_pipeline` DAG in one process with `airflow dags test sales_pipeline`. What is printed now?

**Answer in your notebook:** why is `tasks test` the safest way to try a new task?

## Part 4: Variables from the CLI

1. Create a Variable `environment` with the value `dev`. Then read it back and list all Variables.
2. Test the task `read_variable` of `variables_demo` (any date). What does it print?
3. Change `environment` to `prod` and test the same task again.
4. Delete the Variable and test the task a third time. What does it print, and why?

## Part 5: Challenge (no commands given)

Use `airflow <command> --help` to find the answers.

1. Unpause and trigger `etl_data_pipeline`. When it finishes, show **only its successful runs**.
2. Print the list of its runs as **JSON**.
3. Print the DAG list as a plain, borderless table (hint: look for the `-o` option).
4. Pause every DAG you unpaused during this practice.

---

## Cheat sheet

| I want to... | Command |
|---|---|
| see the Airflow version | `airflow version` |
| list all DAGs | `airflow dags list` |
| find broken DAG files | `airflow dags list-import-errors` |
| see the tasks of a DAG | `airflow tasks list <dag_id> --tree` |
| turn a DAG on / off | `airflow dags unpause <dag_id>` / `airflow dags pause <dag_id>` |
| start a run now | `airflow dags trigger <dag_id>` |
| list the runs of a DAG | `airflow dags list-runs -d <dag_id>` |
| see the state of each task in a run | `airflow tasks states-for-dag-run <dag_id> <run_id>` |
| test one task (nothing is recorded) | `airflow tasks test <dag_id> <task_id> <date>` |
| run the whole DAG in one process | `airflow dags test <dag_id>` |
| set / get / list / delete a Variable | `airflow variables set\|get\|list\|delete ...` |

The solutions, with the output you should see, are in
[`solutions/cli_practice_solution.md`](solutions/cli_practice_solution.md). Try on your own first.
