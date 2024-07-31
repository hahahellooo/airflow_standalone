from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, PythonVirtualenvOperator
with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
   # schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=["movie"],
) as dag:
    
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))


    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)
        print("*"*33)
        print(df.head(10))
        print("*"*33)
        print(df.dtypes)
        
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(df)


    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f"{home_dir}/tmp/test_parquet/load_dt={ds_nodash}"

        #print("=" * 20)
        #print(f"PATH={path}")
        #print("=" * 20)
        #path가 제대로 쓰였는지 확인하기 위해 사용했음 
        #변경 {{ ds_nodash }} -> {ds_nodash}
        if os.path.exists(path):
        # path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ld}")
            return "rm.dir" # "<task_id>" # rf_dir.task_id 로 해도 동작함
        else:
            return "get.data", "echo.task" #if를 False로 만들어서 else가 항상 실행되도록 수정한 뒤 airflow 확인
            


    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
    )

    get_data = PythonVirtualenvOperator(task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule='all_done',
        #venv_cache_path="/home/hahahellooo/tmp2/airflow_venv/get_data" 캐시삭제했기 때문에 매번 새로운 가상환경이 생긴다. 
    )
    
    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule='one_success',
        #venv_cache_path="/home/hahahellooo/tmp2/airflow_venv/get_data"
    )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}'
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'",
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")
    join_task = BashOperator(
        task_id='join',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    start >> branch_op
    start >> join_task >> save_data

    branch_op >> rm_dir >> get_data
    branch_op >> echo_task >> save_data
    branch_op >> get_data


    get_data >> save_data >> end

