# Check Airflow version
airflow version

# List all available DAGs
airflow dags list

# Show detailed information about a specific DAG
airflow dags show example_dag_id

# Check DAG syntax without running it
airflow dags parse /path/to/dag/file.py

# Run a single task
airflow tasks run example_dag_id task_id 2023-01-01

# Trigger a full DAG run
airflow dags trigger example_dag_id

# View DAG run history
airflow dags list-runs -d example_dag_id

# List defined connections
airflow connections list

# Add a new connection
airflow connections add my_conn \
    --conn-type mysql \
    --conn-host localhost \
    --conn-login airflow \
    --conn-password airflow \
    --conn-port 3306

# Manage variables
airflow variables list
airflow variables get my_variable
airflow variables set my_variable my_value

# Initialize the Airflow database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the Web UI server
airflow webserver

# Start the Scheduler
airflow scheduler

# Check Airflow components status
airflow info