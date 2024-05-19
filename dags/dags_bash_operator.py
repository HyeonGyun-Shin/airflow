from airflow import DAG
import datetime
import pendulum # datetime 타입을 좀 더 쉽게 사용할 수 있게 해주는 라이브러리
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_operator", # 일반적으로 파일 이름과 일치시키는 것이 좋음.
    schedule="0 0 * * *", # 분 시 일 월 요일
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"), # DAG이 언제부터 돌건지 결정
    catchup=False, # 
    #dagrun_timeout=datetime.timedelta(minutes=60), # 60분 이상 돌 경우 실패
    #tags=["example", "example2"],
    #params={"example_key": "example_value"},
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2