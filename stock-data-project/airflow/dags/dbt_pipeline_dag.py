from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'aniket',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dbt_stock_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # every 5 minutes
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dags/../../stock_dbt_project && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dags/../../stock_dbt_project && dbt test'
    )

    dbt_run >> dbt_test