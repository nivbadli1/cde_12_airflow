from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
import time 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('ecommerce_sales_pipeline',
         default_args=default_args,
         schedule_interval='0 0 * * *',
         catchup=False) as dag:

    def extract_sales_data():
        # Load data from a CSV file
        try:
            sales_data = pd.read_csv('/tmp/source_data/sales_data.csv')
            return sales_data
        except Exception as e:
            raise ValueError(f"Error loading sales data from CSV: {e}")

    def transform_sales_data(ti):
        try:
            time.sleep(10)
            sales_data = ti.xcom_pull(task_ids='extract_sales_data')
            
            # Data transformation logic
            # 1. Convert order_date column to datetime
            sales_data['order_date'] = pd.to_datetime(sales_data['order_date'])
            
            # 2. Calculate the total revenue for each order
            sales_data['total_revenue'] = sales_data['quantity'] * sales_data['price']
            
            # 3. Group the data by product and calculate the total quantity and revenue
            product_stats = sales_data.groupby('product').agg({
                'quantity': 'sum',
                'total_revenue': 'sum'
            }).reset_index()
            
            return product_stats
        except Exception as e:
            raise ValueError(f"Error transforming sales data: {e}")

    def load_data(ti):
        # Write the transformed data to a new CSV file
        try:
            sales_data = ti.xcom_pull(task_ids='transform_sales_data')
            output_file = '/tmp/target_data/transformed_sales_data.csv'
            sales_data.to_csv(output_file, index=False)
        except Exception as e:
            raise ValueError(f"Error writing transformed sales data to CSV: {e}")


    extract_task = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_sales_data
    )

    transform_task = PythonOperator(
        task_id='transform_sales_data',
        python_callable=transform_sales_data
    )

    load_task = PythonOperator(
        task_id='load_sales_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task