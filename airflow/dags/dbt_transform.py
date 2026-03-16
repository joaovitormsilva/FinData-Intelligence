"""
DAG: dbt_pipeline
Runs dbt seed → run → test via BashOperator.
"""

import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"

def dag_succes_alert(context):
    logging.info(f'Dag has succeeded, run_id: {context.get('task_instance').dag_id}')


def dag_failure_alert(context):
    logging.error(f'Dag has failed, run_id{context.get('task_instance').dag_id}')

@dag(
    dag_id="dbt_transform",
    schedule="@daily",
    on_success_callback=dag_succes_alert,
    on_failure_callback=dag_failure_alert,
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
    
    bsh_cmd = f'cd /opt/dbt && dbt run --select gold.* --project-dir /opt/dbt --profiles-dir /opt/dbt'
    gold_run = BashOperator(
        task_id='gold_run',
        bash_command=bsh_cmd)
    
    bsh_cmd = f'cd /opt/dbt && dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
    test_run = BashOperator(
        task_id='test_run',
        bash_command=bsh_cmd
    )
    
    
    seed >> staging_run >> silver_run >> gold_run >> test_run
 
dbt_pipeline()