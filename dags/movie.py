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
        'retry_delay': timedelta(seconds=3),
    },
    max_active_tasks=3,
    max_active_runs=1,
    description='movie',
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

    def fun_multi_y(ds_nodash, arg1):#독립영화 다양성영화 
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=arg1) 
        print(df[['movieNm','load_dt']].head(5))
        
   # def fun_multi_n(ds_nodash):
   #     from mov.api.call import save2df
   #     p = {"multiMovieYn": "N"}
   #     df = save2df(load_dt=ds_nodash, url_param=p)
   #     print(df[['movieNm','load_dt']].head(5))
    
   # def fun_nation_k(ds_nodash):
   #     from mov.api.call import save2df
   #     p = {"repNationCd": "K"}
   #     df = save2df(load_dt=ds_nodash, url_param=p)
   #     print(df[['movieNm','load_dt']].head(5))

   # def fun_nation_f(ds_nodash):
   #     from mov.api.call import save2df
   #     p = {"repNationCd": "F"}
   #     df = save2df(load_dt=ds_nodash, url_param=p)
   #     print(df[['movieNm','load_dt']].head(5))
    
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
        if os.path.exists(path):
            return "rm.dir" 
        else:
            return "get.start", "echo.task" 
            


    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )

    get_data = PythonVirtualenvOperator(task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule='all_done'
    )
    
    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule='one_success'
    )
    
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        op_kwargs = {'arg1' : {"multiMovieYn": "Y"}},
        system_site_packages=False
            ) # 다양성 영화 유무

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        op_kwargs = {'arg1' : {"multiMovieYn": "N"}},
        system_site_packages=False
    )
    
    nation_k = PythonVirtualenvOperator(
        task_id='nation_k',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        op_kwargs = {'arg1' : {"repNationCd": "K"}},
        system_site_packages=False
    )
    
    nation_f = PythonVirtualenvOperator(
        task_id='nation_f',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/hahahellooo/mov.git@0.3/api"],
        op_kwargs = {'arg1' : {"repNationCd": "F"}},
        system_site_packages=False
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
    end = EmptyOperator(task_id='end')
   
    get_start = EmptyOperator(
            task_id='get.start', 
            trigger_rule='all_done'
    )
    
    get_end = EmptyOperator(task_id='get.end')

    throw_err = BashOperator(
        task_id='throw.err',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start
    branch_op >> echo_task >> get_start 
    get_start >>  [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end
   
    get_end  >> save_data >> end

