"""
SOLUTION - Exercise 1: Support tickets ETL (basic)

extract -> transform -> load, using XCom to pass FILE PATHS (not data) between tasks,
and Airflow Variables for configuration.

Variables (all optional - defaults are used if they do not exist):
  output_dir        default: /tmp/target_data
  default_priority  default: medium
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

SOURCE_FILE = "/tmp/source_data/support_tickets.csv"

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def extract():
    """Copy the raw file to a staging area and return its path."""
    output_dir = Variable.get("output_dir", default_var="/tmp/target_data")
    staging_dir = os.path.join(output_dir, "staging")
    os.makedirs(staging_dir, exist_ok=True)

    df = pd.read_csv(SOURCE_FILE)
    print(f"Read {len(df)} rows from {SOURCE_FILE}")

    staging_path = os.path.join(staging_dir, "tickets_raw.csv")
    df.to_csv(staging_path, index=False)
    return staging_path


def transform(ti):
    """Clean the data: normalize text, remove duplicates, fill missing priority."""
    staging_path = ti.xcom_pull(task_ids="extract")
    df = pd.read_csv(staging_path)
    print(f"Rows before cleaning: {len(df)}")

    # 1. Text columns: remove spaces, lower case ("  Billing " -> "billing")
    for col in ["category", "priority", "status"]:
        df[col] = df[col].astype("string").str.strip().str.lower()

    # 2. Remove duplicated tickets (same ticket_id)
    df = df.drop_duplicates(subset="ticket_id", keep="first")

    # 3. Missing priority -> value from a Variable
    default_priority = Variable.get("default_priority", default_var="medium")
    missing = int(df["priority"].isna().sum())
    df["priority"] = df["priority"].fillna(default_priority)
    print(f"Filled {missing} missing priorities with '{default_priority}'")

    # 4. New column
    df["is_high_priority"] = df["priority"].isin(["high", "urgent"])

    print(f"Rows after cleaning: {len(df)}")
    clean_path = staging_path.replace("tickets_raw.csv", "tickets_clean.csv")
    df.to_csv(clean_path, index=False)
    return clean_path


def load(ti):
    """Build a summary per category and write it to the output folder."""
    clean_path = ti.xcom_pull(task_ids="transform")
    df = pd.read_csv(clean_path)

    summary = (
        df.groupby("category")
        .agg(
            tickets=("ticket_id", "count"),
            high_priority=("is_high_priority", "sum"),
            avg_resolution_hours=("resolution_hours", "mean"),
        )
        .round(1)
        .reset_index()
    )

    output_dir = Variable.get("output_dir", default_var="/tmp/target_data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "ticket_summary.csv")
    summary.to_csv(output_path, index=False)

    print(summary.to_string(index=False))
    print(f"Summary written to {output_path}")


with DAG(
    dag_id="ex1_support_tickets_etl",
    default_args=default_args,
    description="Exercise 1 - basic ETL with XCom and Variables",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["exercise", "basic"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
