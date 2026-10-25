"""
SOLUTION - Exercise 2: Weather sensors with data-quality gates (intermediate)

Three cities are processed in parallel. Each city has its own extract task and its own
quality-check task. Only when all checks pass, the data is merged and loaded.

Variables (optional):
  max_null_pct   default: 25    (maximum allowed percentage of missing values per city)
  output_dir     default: /tmp/target_data

Try it: set max_null_pct = 10 and run again. Jerusalem's sensor is flaky, so its
quality check fails, and the tasks after it get the state "upstream_failed".
"""
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

SOURCE_DIR = "/tmp/source_data"
CITIES = ["tel_aviv", "haifa", "jerusalem"]
MEASURES = ["temperature_c", "humidity_pct"]

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _output_dir():
    return Variable.get("output_dir", default_var="/tmp/target_data")


def extract_city(city):
    """Read one city's file, clean the sentinel value -999, report the % of missing values."""
    df = pd.read_csv(os.path.join(SOURCE_DIR, f"sensors_{city}.csv"))

    # The device writes -999 when a measurement failed -> that means "missing"
    df[MEASURES] = df[MEASURES].replace(-999, np.nan)

    total_cells = df[MEASURES].size
    null_pct = round(float(df[MEASURES].isna().sum().sum()) / total_cells * 100, 1)
    print(f"{city}: {len(df)} rows, {null_pct}% missing values")

    staging_dir = os.path.join(_output_dir(), "staging")
    os.makedirs(staging_dir, exist_ok=True)
    path = os.path.join(staging_dir, f"sensors_{city}_clean.csv")
    df.to_csv(path, index=False)

    # Small, JSON-friendly dictionary -> safe for XCom
    return {"city": city, "path": path, "rows": int(len(df)), "null_pct": null_pct}


def quality_check(city, ti):
    """Fail the task if the city has too many missing values."""
    info = ti.xcom_pull(task_ids=f"extract_{city}")
    max_null_pct = float(Variable.get("max_null_pct", default_var="25"))

    if info["null_pct"] > max_null_pct:
        raise ValueError(
            f"Quality check FAILED for {city}: {info['null_pct']}% missing values "
            f"(allowed: {max_null_pct}%)"
        )
    print(f"Quality check passed for {city}: {info['null_pct']}% <= {max_null_pct}%")


def merge_and_load(ti):
    """Merge the three cities and write a daily summary."""
    infos = ti.xcom_pull(task_ids=[f"extract_{c}" for c in CITIES])

    frames = []
    for info in infos:
        part = pd.read_csv(info["path"])
        part["city"] = info["city"]
        frames.append(part)
    df = pd.concat(frames, ignore_index=True)

    df["reading_time"] = pd.to_datetime(df["reading_time"])
    df["date"] = df["reading_time"].dt.date

    summary = (
        df.groupby(["city", "date"])
        .agg(
            readings=("reading_time", "count"),
            avg_temp=("temperature_c", "mean"),
            min_temp=("temperature_c", "min"),
            max_temp=("temperature_c", "max"),
            avg_humidity=("humidity_pct", "mean"),
        )
        .round(1)
        .reset_index()
    )

    output_dir = _output_dir()
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "daily_weather_summary.csv")
    summary.to_csv(output_path, index=False)
    print(summary.head(9).to_string(index=False))
    print(f"Wrote {len(summary)} rows to {output_path}")


def finish():
    print("Pipeline finished (check the Grid view to see which cities passed the quality check).")


with DAG(
    dag_id="ex2_sensor_quality_pipeline",
    default_args=default_args,
    description="Exercise 2 - parallel extraction with data-quality gates",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["exercise", "intermediate"],
) as dag:

    start = EmptyOperator(task_id="start")

    merge_task = PythonOperator(task_id="merge_and_load", python_callable=merge_and_load)

    # Always runs at the end, even if something failed upstream
    end = PythonOperator(
        task_id="finish",
        python_callable=finish,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # One extract task + one quality-check task per city (created with a loop)
    for city in CITIES:
        extract_task = PythonOperator(
            task_id=f"extract_{city}",
            python_callable=extract_city,
            op_kwargs={"city": city},
        )
        check_task = PythonOperator(
            task_id=f"quality_check_{city}",
            python_callable=quality_check,
            op_kwargs={"city": city},
        )
        start >> extract_task >> check_task >> merge_task

    merge_task >> end
