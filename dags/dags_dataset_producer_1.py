from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# 실제 큐가 있는 것은 아니고 DB에 Produce / Consume 내역을 기록

# 큐에서 퍼블리쉬 할때 "dags_dataset_producer_1" 로 이름을 준다
dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
        dag_id='dags_dataset_producer_1',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1],
        bash_command='echo "producer_1 수행 완료"'
    )