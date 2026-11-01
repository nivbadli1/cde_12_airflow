"""
Exercise 4 - STARTER: run a DAG and check the result from a Python script.

Copy this file to  target_data/run_and_check.py  (on your machine), fill in the TODOs, and run it
INSIDE the container, where the `airflow` command exists:

    docker compose exec airflow-scheduler python /tmp/target_data/run_and_check.py

Exit codes your script must use:
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


# ------------------------------------------------------------------ helpers (already written)
def airflow(*args):
    """Run `airflow <args>` and return (exit_code, stdout).
    Example: airflow("dags", "list", "-o", "json")"""
    result = subprocess.run(["airflow", *args], capture_output=True, text=True)
    return result.returncode, result.stdout


def parse_json(text):
    """Airflow sometimes prints a log line before the JSON. Skip everything before the first '['."""
    return json.loads(text[text.index("["):])


# ------------------------------------------------------------------ your work
def dag_exists(dag_id):
    """TODO 1: return True if `dag_id` is in the output of `airflow dags list -o json`."""
    raise NotImplementedError


def has_import_errors():
    """TODO 2: return True if `airflow dags list-import-errors` reports a problem.
    Hint: when everything is fine it prints "No data found"."""
    raise NotImplementedError


def trigger(dag_id):
    """TODO 3: unpause the DAG, then start a run with your OWN run id, and return that run id.
    Hints: airflow dags unpause <dag_id>
           airflow dags trigger <dag_id> --run-id <run_id>
           run_id = f"cli_check_{datetime.now():%Y%m%d_%H%M%S}"   (unique, and you know it in advance)
    If `trigger` returns a non-zero exit code, print its output and exit with code 2."""
    raise NotImplementedError


def wait_for_run(dag_id, run_id, timeout, poll):
    """TODO 4: check `airflow dags list-runs -d <dag_id> -o json` every `poll` seconds.
    Find the run whose "run_id" is yours. Return its "state" as soon as it is "success" or "failed".
    Return None if `timeout` seconds pass first."""
    raise NotImplementedError


def print_task_states(dag_id, run_id):
    """TODO 5: print the state of every task, using
    airflow tasks states-for-dag-run <dag_id> <run_id> -o json"""
    raise NotImplementedError


def main():
    parser = argparse.ArgumentParser(description="Run a DAG from the CLI and check the result.")
    parser.add_argument("--dag-id", default="nightly_report")
    parser.add_argument("--timeout", type=int, default=120, help="seconds to wait for the run")
    parser.add_argument("--poll", type=int, default=3, help="seconds between checks")
    args = parser.parse_args()

    # TODO 6: put the steps together.
    #   1. DAG missing                -> print an error, exit code 2
    #   2. import errors              -> print an error, exit code 2
    #   3. trigger, print the run id
    #   4. wait; on timeout           -> print a message, exit code 3
    #   5. print the final state and the task states
    #   6. exit 0 if the state is "success", otherwise exit 1
    raise NotImplementedError


if __name__ == "__main__":
    main()
