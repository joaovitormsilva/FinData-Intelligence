"""
DAG: dbt_pipeline
Runs dbt seed → run → test via BashOperator.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"





@dag(
    dag_id="dbt_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "financas"],
)
def dbt_pipeline():
    
   
    bsh_cmd = f'cd /opt/dbt && dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
    BashOperator(
        task_id='test_model',
        bash_command=bsh_cmd
    )
 
dbt_pipeline()