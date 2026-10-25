"""
SOLUTION - Exercise 3: Shipments pipeline, production style (advanced)

Real-world patterns in one DAG:
  * input check            - fail early if the file is missing
  * validation + reject file - bad rows are saved for audit, not silently dropped
  * quality gate           - stop the pipeline if too many rows are bad
  * retries                - the "warehouse" fails on the first attempt (simulated)
  * idempotency            - output file name contains the run date (ds); re-running overwrites
  * atomic write           - write to a temp file, then rename
  * run metadata           - save the last successful date in a Variable
  * notify + cleanup       - run ALWAYS (trigger_rule=ALL_DONE), even after a failure

Variables (all optional):
  max_reject_pct      default: 10     quality gate threshold
  sla_days            default: 5      a delivery slower than this is "late"
  simulate_flaky_load default: true   first load attempt fails -> shows retries
  output_dir          default: /tmp/target_data
Written by the pipeline:
  shipments_last_success_date
"""
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SOURCE_FILE = "/tmp/source_data/shipments.csv"

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}


def _output_dir():
    return Variable.get("output_dir", default_var="/tmp/target_data")


def _staging_dir():
    path = os.path.join(_output_dir(), "staging")
    os.makedirs(path, exist_ok=True)
    return path


# ---------------------------------------------------------------- tasks
def check_source_file():
    if not os.path.exists(SOURCE_FILE) or os.path.getsize(SOURCE_FILE) == 0:
        raise FileNotFoundError(f"Source file is missing or empty: {SOURCE_FILE}")
    print(f"Source file OK ({os.path.getsize(SOURCE_FILE)} bytes)")


def extract_and_validate(ds):
    """Split the file into valid rows and rejected rows (with a reason)."""
    df = pd.read_csv(SOURCE_FILE, dtype=str)
    total = len(df)

    df["shipped_dt"] = pd.to_datetime(df["shipped_date"], errors="coerce")
    df["delivered_dt"] = pd.to_datetime(df["delivered_date"], errors="coerce")
    df["weight_num"] = pd.to_numeric(df["weight_kg"], errors="coerce")

    missing_id = df["shipment_id"].isna() | (df["shipment_id"].str.strip() == "")
    bad_weight = df["weight_num"].isna() | (df["weight_num"] <= 0)
    bad_dates = df["shipped_dt"].isna() | (
        df["delivered_dt"].notna() & (df["delivered_dt"] < df["shipped_dt"])
    )

    # The first matching reason wins
    df["reject_reason"] = np.select(
        [missing_id, bad_weight, bad_dates],
        ["missing_shipment_id", "invalid_weight", "invalid_dates"],
        default="",
    )

    rejected = df[df["reject_reason"] != ""]
    valid = df[df["reject_reason"] == ""].copy()

    staging = _staging_dir()
    rejected_path = os.path.join(staging, f"rejected_{ds}.csv")
    valid_path = os.path.join(staging, f"valid_{ds}.csv")
    rejected.to_csv(rejected_path, index=False)
    valid.to_csv(valid_path, index=False)

    stats = {
        "total": int(total),
        "valid": int(len(valid)),
        "rejected": int(len(rejected)),
        "reject_pct": round(len(rejected) / total * 100, 1),
        "valid_path": valid_path,
        "rejected_path": rejected_path,
    }
    print(f"Validation result: {stats}")
    print(rejected["reject_reason"].value_counts().to_string())
    return stats


def quality_gate(ti):
    stats = ti.xcom_pull(task_ids="extract_and_validate")
    max_reject_pct = float(Variable.get("max_reject_pct", default_var="10"))
    if stats["reject_pct"] > max_reject_pct:
        raise ValueError(
            f"Quality gate FAILED: {stats['reject_pct']}% rejected rows "
            f"(allowed: {max_reject_pct}%)"
        )
    print(f"Quality gate passed: {stats['reject_pct']}% <= {max_reject_pct}%")


def transform(ti, ds):
    """Delivery time per carrier + late-delivery percentage."""
    stats = ti.xcom_pull(task_ids="extract_and_validate")
    sla_days = int(Variable.get("sla_days", default_var="5"))

    df = pd.read_csv(stats["valid_path"], parse_dates=["shipped_dt", "delivered_dt"])
    df["carrier"] = df["carrier"].str.strip().str.upper()

    in_transit = int(df["delivered_dt"].isna().sum())
    delivered = df[df["delivered_dt"].notna()].copy()
    delivered["delivery_days"] = (delivered["delivered_dt"] - delivered["shipped_dt"]).dt.days
    delivered["is_late"] = delivered["delivery_days"] > sla_days

    report = (
        delivered.groupby("carrier")
        .agg(
            shipments=("shipment_id", "count"),
            avg_delivery_days=("delivery_days", "mean"),
            late_shipments=("is_late", "sum"),
        )
        .reset_index()
    )
    report["avg_delivery_days"] = report["avg_delivery_days"].round(1)
    report["late_pct"] = (report["late_shipments"] / report["shipments"] * 100).round(1)

    report_path = os.path.join(_staging_dir(), f"carrier_report_{ds}.csv")
    report.to_csv(report_path, index=False)
    print(f"SLA = {sla_days} days, in transit (not counted): {in_transit}")
    print(report.to_string(index=False))
    return {"report_path": report_path, "in_transit": in_transit}


def load_report(ti, ds):
    """'Load' the report. The first attempt fails on purpose to demonstrate retries."""
    simulate = Variable.get("simulate_flaky_load", default_var="true") == "true"
    if simulate and ti.try_number == 1:
        raise ConnectionError("Warehouse temporarily unavailable (simulated) - Airflow will retry")

    info = ti.xcom_pull(task_ids="transform")
    final_path = os.path.join(_output_dir(), f"carrier_report_{ds}.csv")
    tmp_path = final_path + ".tmp"

    # Atomic write: readers never see a half-written file
    pd.read_csv(info["report_path"]).to_csv(tmp_path, index=False)
    os.replace(tmp_path, final_path)
    print(f"Report loaded to {final_path} (attempt number {ti.try_number})")


def save_run_metadata(ds):
    Variable.set("shipments_last_success_date", ds)
    print(f"Variable shipments_last_success_date = {ds}")


def notify(dag_run):
    """Runs always. Reports which tasks did not succeed."""
    problems = [
        f"{t.task_id}={t.state}"
        for t in dag_run.get_task_instances()
        if t.state in ("failed", "upstream_failed")
    ]
    if problems:
        print("PIPELINE FINISHED WITH PROBLEMS: " + ", ".join(problems))
    else:
        print("PIPELINE FINISHED SUCCESSFULLY")


def cleanup(ds):
    """Runs always. Remove the temporary valid-rows file (keep the rejected file for audit)."""
    path = os.path.join(_staging_dir(), f"valid_{ds}.csv")
    if os.path.exists(path):
        os.remove(path)
        print(f"Removed {path}")


# ---------------------------------------------------------------- DAG
with DAG(
    dag_id="ex3_shipments_production_pipeline",
    default_args=default_args,
    description="Exercise 3 - production-style pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["exercise", "advanced"],
) as dag:

    t_check = PythonOperator(task_id="check_source_file", python_callable=check_source_file)
    t_extract = PythonOperator(task_id="extract_and_validate", python_callable=extract_and_validate)
    t_gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load_report", python_callable=load_report)
    t_meta = PythonOperator(task_id="save_run_metadata", python_callable=save_run_metadata)

    t_notify = PythonOperator(
        task_id="notify", python_callable=notify, trigger_rule=TriggerRule.ALL_DONE
    )
    t_cleanup = PythonOperator(
        task_id="cleanup", python_callable=cleanup, trigger_rule=TriggerRule.ALL_DONE
    )

    t_check >> t_extract >> t_gate >> t_transform >> t_load >> t_meta >> t_notify >> t_cleanup
