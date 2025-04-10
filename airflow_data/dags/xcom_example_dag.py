"""
Simple example demonstrating XCom usage in Airflow
"""
from datetime import datetime
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

# Create the DAG
dag = DAG(
    'xcom_example_dag',
    description='Simple example demonstrating XCom',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Function that generates a random number and saves it to XCom
def generate_random_number(**kwargs):
    """Generates a random number between 1 and 100 and stores it in XCom."""
    random_number = random.randint(1, 100)
    print(f"#########################################")
    print(f"Generated random number: {random_number}")
    print(f"#########################################")
    # Save the value to XCom
    # kwargs['ti'] represents the current task instance
    kwargs['ti'].xcom_push(key='random_number', value=random_number)
    
    return random_number

# Function that reads the random number from XCom, multiplies it by 10, and saves the result to XCom
def multiply_by_ten(**kwargs):
    """Reads the random number from XCom, multiplies it by 10, and saves the result."""
    ti = kwargs['ti']
    
    # Pull the value from XCom
    random_number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    
    result = random_number * 10
    print(f"#########################################")
    print(f"Number {random_number} after multiplication by 10: {result}")
    print(f"#########################################")
    
    # Save the result to XCom
    ti.xcom_push(key='multiplied_number', value=result)
    
    return result

# Function that displays the final result
def print_result(**kwargs):
    """Reads the multiplied value from XCom and displays it."""
    ti = kwargs['ti']
    
    # Pull the multiplied value from XCom
    multiplied_number = ti.xcom_pull(task_ids='multiply_number', key='multiplied_number')
    
    # Also pull the original number for comparison
    original_number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    print(f"#########################################")
    print(f"Original number: {original_number}")
    print(f"Final result after multiplication: {multiplied_number}")
    print(f"#########################################")
    return f"Process completed: {original_number} -> {multiplied_number}"

# Create the tasks
generate_number = PythonOperator(
    task_id='generate_number',
    python_callable=generate_random_number,
    provide_context=True,
    dag=dag
)

multiply_number = PythonOperator(
    task_id='multiply_number',
    python_callable=multiply_by_ten,
    provide_context=True,
    dag=dag
)

show_result = PythonOperator(
    task_id='show_result',
    python_callable=print_result,
    provide_context=True,
    dag=dag
)

# Set the task dependencies
generate_number >> multiply_number >> show_result