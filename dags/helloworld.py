from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
   # t1 = BashOperator(
   #    task_id='print_date',
   #    bash_command='date',
   # )

   # t2 = BashOperator(
     #   task_id='sleep',
      #  depends_on_past=False,
       # bash_command='sleep 5',
       # retries=3,
   # )



#    t3 = DummyOperator(task_id='t3')
#    t33 = DummyOperator(task_id='t33')
#    t22 = DummyOperator(task_id='t22')
#    task_end = DummyOperator(task_id='end')
#    task_start = DummyOperator(task_id='start')
#    task_empty = DummyOperator(task_id='empty')

#    t1 >> [t2, t3] >> task_end
#    task_start >> t1
#    t3 >> task_empty
#    t1 >> t22 >> task_end 
#    t1 >> t33 >> task_end
#    task_empty >> task_end

     
    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end')
    get_data = DummyOperator(task_id='get_data')
    data_2021 = DummyOperator(task_id='data_2021')
    data_2022 = DummyOperator(task_id='data_2022')
    data_2023 = DummyOperator(task_id='data_2023')
    sort_2021 = DummyOperator(task_id='sort_2021')
    sort_2022 = DummyOperator(task_id='sort_2022')
    sort_2023 = DummyOperator(task_id='sort_2023')
    best_book = DummyOperator(task_id='best_book')
    popup_reco = DummyOperator(task_id='popup_reco')
    task_start >> get_data >> [data_2021, data_2022, data_2023]
    data_2021 >> sort_2021
    data_2022 >> sort_2022
    data_2023 >> sort_2023
    [sort_2021, sort_2022, sort_2023] >> best_book >> popup_reco
    popup_reco >> task_end
