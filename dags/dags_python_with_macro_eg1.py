from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False, 
) as dag:
    
    @task(task_id='task_using_macros'
        templates_dict={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                        'end_date':'{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
                        }      
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date 없음'
            end_date = templates_dict.get('end_date') or 'end_date 없음'
            print(start_date)
            print(end_date)


    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        date_interval_end = kwargs['data_interval_end']
        prev_month_day_first = date_interval_end.in_timezone('Asia/Seoul') + relativedelta(month=-1, day=1)
        prev_month_day_last = date_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(day=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()