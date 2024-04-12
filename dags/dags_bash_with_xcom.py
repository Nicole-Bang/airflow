from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

#bash 오퍼레이터는 env, bash_command 파라미터에서 Template 이용하여 push/pull

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo START && "
                 "echo XCOM_PUSHED "
                 "{{ ti.xcom_push(key='bash_pushed',value='first_bash_message') }} && "
                 "echo COMPLETE"    # 마지막 출력문은 자동으로 return_value에 저장
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",    # first_bash_message
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"}, # COMPLETE
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False  # bash_command를 xcom에 올리지 말라는 뜻 true 또는 명시가 없을때는 xcom에 저장된다
    )

    bash_push >> bash_pull