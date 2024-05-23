from airflow import DAG
import datetime
import pendulum 
from airflow.decorators import task



with DAG(
    dag_id="dags_python_show_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 5, 10, tz="Asia/Seoul"),
    catchup=True, 
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()