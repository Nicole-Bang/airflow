# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

# Task 수준의 timeout 과 DAG 수준의 timeout이 존재
# 이 실습은 Task 수준의 timeout

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_1',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=20),
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    bash_sleep_30 = BashOperator(
        task_id='bash_sleep_30',        # 이 task는 30초동안 슬립함. 즉, timeout은 20초이기 때문에 fail 할꺼임
        bash_command='sleep 30',
    )

    bash_sleep_10 = BashOperator(
        trigger_rule='all_done',    # 실패, 성공 여부에 관계없이 선행 task가 돌기만 하면 후행이 실행됨
        task_id='bash_sleep_10',
        bash_command='sleep 10',
    )
    bash_sleep_30 >> bash_sleep_10
    
    