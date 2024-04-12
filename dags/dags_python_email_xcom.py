from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.email import EmailOperator

# Email 오퍼레이터는 어떤 파라미터에 Template을 사용할 수 있나요??
# to, subject, html_comtent, files : line 25부터 참고

with DAG(
    dag_id="dags_python_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice 
        return choice(['Success','Fail']) #random 라이브러리의 choice -> 아무값이나 랜덤하게 선택되는 것


    send_email = EmailOperator(
        task_id='send_email',
        to='ausdl88@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                    {{ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>'
    )

    some_logic() >> send_email