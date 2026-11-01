"""
Exercise 4 - SOLUTION: run a DAG and check the result from a Python script.

Run it INSIDE the container (that is where the `airflow` command exists):

    docker compose exec airflow-scheduler python /tmp/target_data/run_and_check.py
    docker compose exec airflow-scheduler python /tmp/target_data/run_and_check.py --dag-id sales_pipeline

Exit codes (this is how cron, CI or another script knows what happened):
    0  the run finished with state "success"
    1  the run finished with state "failed"
    2  setup problem: the DAG does not exist or a DAG file has an import error
    3  timeout: the run did not finish in time
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime


def airflow(*args):
    """Run `airflow <args>` and return (exit_code, stdout)."""
    result = subprocess.run(["airflow", *args], capture_output=True, text=True)
    return result.returncode, result.stdout


def parse_json(text):
    """Airflow sometimes prints a log line before the JSON. Skip everything before the first '['."""
    return json.loads(text[text.index("["):])


# ---------------------------------------------------------------- step 1
def dag_exists(dag_id):
    _, out = airflow("dags", "list", "-o", "json")
    return any(d["dag_id"] == dag_id for d in parse_json(out))


# ---------------------------------------------------------------- step 2
def has_import_errors():
    _, out = airflow("dags", "list-import-errors")
    return "No data found" not in out


# ---------------------------------------------------------------- step 3 + 4
def trigger(dag_id):
    """Unpause the DAG, start a run with our OWN run_id and return that run_id."""
    airflow("dags", "unpause", dag_id)
    run_id = f"cli_check_{datetime.now():%Y%m%d_%H%M%S}"
    code, out = airflow("dags", "trigger", dag_id, "--run-id", run_id)
    if code != 0:
        print(out)
        sys.exit(2)
    return run_id


# ---------------------------------------------------------------- step 5
def wait_for_run(dag_id, run_id, timeout, poll):
    """Poll `list-runs` until our run is success/failed. Return the final state, or None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, out = airflow("dags", "list-runs", "-d", dag_id, "-o", "json")
        for run in parse_json(out):
            if run["run_id"] == run_id and run["state"] in ("success", "failed"):
                return run["state"]
        time.sleep(poll)
    return None


# ---------------------------------------------------------------- step 6
def print_task_states(dag_id, run_id):
    _, out = airflow("tasks", "states-for-dag-run", dag_id, run_id, "-o", "json")
    for task in parse_json(out):
        print(f"  {task['task_id']:<12} {task['state']}")


def main():
    parser = argparse.ArgumentParser(description="Run a DAG from the CLI and check the result.")
    parser.add_argument("--dag-id", default="nightly_report")
    parser.add_argument("--timeout", type=int, default=120, help="seconds to wait for the run")
    parser.add_argument("--poll", type=int, default=3, help="seconds between checks")
    args = parser.parse_args()

    if not dag_exists(args.dag_id):
        print(f"ERROR: DAG '{args.dag_id}' does not exist")
        sys.exit(2)
    if has_import_errors():
        print("ERROR: a DAG file has an import error (run: airflow dags list-import-errors)")
        sys.exit(2)

    run_id = trigger(args.dag_id)
    print(f"Triggered {args.dag_id}, run_id = {run_id}")

    state = wait_for_run(args.dag_id, run_id, args.timeout, args.poll)
    if state is None:
        print(f"TIMEOUT: the run did not finish in {args.timeout} seconds")
        sys.exit(3)

    print(f"Run finished with state: {state}")
    print_task_states(args.dag_id, run_id)
    sys.exit(0 if state == "success" else 1)


if __name__ == "__main__":
    main()
