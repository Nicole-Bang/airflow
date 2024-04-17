from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

# Task 수준의 timeout 과 DAG 수준의 timeout이 존재
# 이 실습은 DAG 수준의 timeout임

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_2',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=40), 
        # DAG 수준에서의 fail이 나타나지만 execution_timeout는 task 수준의 설정(파라미터)이므로 이메일은 발송되지 않는다
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    bash_sleep_35 = BashOperator(
        task_id='bash_sleep_35',
        bash_command='sleep 35',
    )

    bash_sleep_36 = BashOperator(
        trigger_rule='all_done',
        task_id='bash_sleep_36',
        bash_command='sleep 36',
    )

    bash_go = BashOperator(
        task_id='bash_go',
        bash_command='exit 0',
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go
    # bash_sleep_35 는 35초간 sleep, bash_sleep_36는 36초간 sleep으로 dag 수준의 timeout인 60초를 초과하였으므로 fail
    