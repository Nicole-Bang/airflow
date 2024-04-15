# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

# ExternalTask : 다른 dag의 완료를 감지하여 본 task를 수행 -> dag B의 센서a로 dag A의 완료를 감지한 후 dag B의 Task B 수행
# TriggerDagRun : 실행할 다른 dag의 id를 지정하여 수행 -> dag A의 task a의 실행 후 task b를 수행할 때 dag B를 수행  

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',              # 필수값
        trigger_dag_id='dags_python_operator',   # 필수값
        trigger_run_id=None,                    # run_id : dag의 수행방식과 시간을 유일하게 식별해주는 키
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,      # true : 선행된 트리거가 성공해야 후행 task가 실행된다
        poke_interval=60,               # 모니터링 주기
        allowed_states=['success'],     # 트리거에 딸린 덱이 'success'상태로 끝나야 트리거가 포함된 task도 success로 마킹됨
        failed_states=None
        )

    start_task >> trigger_dag_task