from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator, 
        BranchPythonOperator, 
        PythonVirtualenvOperator,

)

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:


    merge_df = EmptyOperator(
        task_id='merge.df'
    
    )
    
    apply_type = EmptyOperator(
        task_id='apply.type'

    )
    
    de_dup = EmptyOperator(
        task_id='de_dup'

    )

    summary_df = EmptyOperator(
            task_id='summary.df'

    )

    start = EmptyOperator(
            task_id='start'
    )
    
    end= EmptyOperator(
            task_id='end'
    )
   

    start >> merge_df >> apply_type >> de_dup >> summary_df >> end
