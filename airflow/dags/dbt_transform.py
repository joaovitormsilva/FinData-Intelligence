"""
DAG: dbt_pipeline
Runs dbt seed → run → test via BashOperator.
"""

import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"

@dag(
    dag_id="dbt_transform",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "financas"],
)
def dbt_pipeline():

    bsh_cmd = f'cd /opt/dbt && dbt seed --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
    seed = BashOperator(task_id='dbt_seed', bash_command=bsh_cmd)

    bsh_cmd = f'cd /opt/dbt && dbt run --select staging.* --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
    staging_run = BashOperator(
        task_id='dbt_staging_run',
        bash_command=bsh_cmd)
  
    bsh_cmd = f'cd /opt/dbt && dbt run --select silver.* --project-dir /opt/dbt --profiles-dir /opt/dbt'
    silver_run = BashOperator(
        task_id='silver_run',
        bash_command=bsh_cmd)

    seed >> staging_run >> silver_run 
 
dbt_pipeline()