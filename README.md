#Introduction to Airflow

## Exercise: Creating a Basic DAG with Python and Bash Operators

For this exercise, we'll focus on a real-world data engineering scenario where you'll need to create a data pipeline to extract, transform, and load sales data from a CSV file into a data warehouse.

### Scenario

Your company has a large e-commerce platform, and you need to regularly fetch sales data from a CSV file and store it in a data warehouse for further analysis. The sales data is provided in a CSV file, and you need to build a data pipeline that can run automatically every day to ensure the data is up-to-date.

### Objectives

1. Create a basic DAG (Directed Acyclic Graph) in Airflow.
2. Use a Python Operator to extract data from a CSV file.
3. Use a Python Operator to transform the data.
4. Use a Python Operator to load the data into a target folder.

### Instructions

1. **Create a new DAG file**: In the `dags` directory of your Airflow installation, create a new file called `ecommerce_sales_pipeline.py`.

2. **Import the necessary modules**: In the new file, import the following modules:

   ```python
   from datetime import datetime, timedelta
   from airflow import DAG
   from airflow.operators.python_operator import PythonOperator
   import pandas as pd
   import psycopg2
   ```

3. **Define the default arguments for the DAG**: Set the default arguments for the DAG, including the owner, start date, and email notification settings.

   ```python
   default_args = {
       'owner': 'airflow',
       'start_date': datetime(2023, 8, 1),
       'email': ['your_email@example.com'],
       'email_on_failure': True,
       'email_on_retry': False,
       'retries': 3,
       'retry_delay': timedelta(minutes=5)
   }
   ```

4. **Create the DAG**: Define the DAG, including the schedule interval and the default arguments.

   ```python
   with DAG('ecommerce_sales_pipeline',
            default_args=default_args,
            schedule_interval='0 0 * * *',
            catchup=False) as dag:
   ```

5. **Define the tasks**: Inside the DAG context, define the following tasks:

   a. **Extract data from a CSV file**: Create a Python Operator that loads the sales data from a CSV file source_data/sales_data.csv .

   ```python
   def extract_sales_data():
       # Load data from a CSV file
       try:
           sales_data = pd.read_csv('tmp/source_data/sales_data.csv')
           return sales_data
       except Exception as e:
           raise ValueError(f"Error loading sales data from CSV: {e}")
   
   extract_task = PythonOperator(
       task_id='extract_sales_data',
       python_callable=extract_sales_data
   )
   ```

   b. **Transform the data**: Create a Python Operator that performs any necessary data transformation on the extracted data.

   ```python
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
   
   transform_task = PythonOperator(
       task_id='transform_sales_data',
       python_callable=transform_sales_data
   )
   ```

   c. **Load data into the data warehouse**: Create a Python Operator that loads the transformed sales data into a PostgreSQL database.

   ```python
    def load_data(ti):
        # Write the transformed data to a new CSV file
        try:
            sales_data = ti.xcom_pull(task_ids='transform_sales_data')
            output_file = '/tmp/target_data/transformed_sales_data.csv'
            sales_data.to_csv(output_file, index=False)
        except Exception as e:
            raise ValueError(f"Error writing transformed sales data to CSV: {e}")
   
   load_task = PythonOperator(
       task_id='load_sales_data',
       python_callable=load_data
   )
   ```

6. **Set the task dependencies**: Define the order in which the tasks should be executed.

   ```python
   extract_task >> transform_task >> load_task
   ```

7. **Save the DAG file**: Save the `ecommerce_sales_pipeline.py` file in the `dags` directory of your Airflow installation.

8. **Generate the CSV file**: Create a `source_data` directory in the same directory as the `ecommerce_sales_pipeline.py` file, and generate a `sales_data.csv` file with the following data:

   ```csv
   order_date,product,quantity,price
   2023-01-01,Product A,10,20.50
   2023-01-02,Product B,5,15.75
   2023-01-03,Product C,8,22.00
   2023-01-04,Product A,12,20.50
   2023-01-05,Product B,7,15.75
   ```

### Answers

1. The `ecommerce_sales_pipeline.py` file should look like this:

   ```python
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
   ```

2. The `sales_data.csv` file should look like this:

   ```csv
   order_date,product,quantity,price
   2023-01-01,Product A,10,20.50
   2023-01-02,Product B,5,15.75
   2023-01-03,Product C,8,22.00
   2023-01-04,Product A,12,20.50
   2023-01-05,Product B,7,15.75
   ```

3. The `transformed_sales_data.cs` file should look like this:

    ```csv
    product,quantity,total_revenue
    Product A,22,451.0
    Product B,12,189.0
    Product C,8,176.0
   ```

3. The DAG has the following components:
   - Default arguments: Sets the owner, start date, email notification settings, and retry parameters.
   - DAG definition: Creates the DAG with the name 'ecommerce_sales_pipeline' and a schedule interval of daily (at midnight).
   - Extract sales data task: Defines a Python Operator that loads the sales data from a CSV file.
   - Transform sales data task: Defines a Python Operator that performs any necessary data transformation on the extracted data.
   - Load sales data task: Defines a Python Operator that write the transformed sales data into a CSV file.
   - Task dependencies: Sets the order of execution, where the 'extract_sales_data' task runs before the 'transform_sales_data' task, which runs before the 'load_sales_data' task.

4. To run the DAG, you need to have Airflow installed with Docker, as mentioned in the instructions. Once the DAG file is saved in the `dags` directory and the `sales_data.csv` file is placed in the `source_data` directory, Airflow will automatically detect and schedule the pipeline to run daily.
