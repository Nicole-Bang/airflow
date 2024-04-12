from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Variable
# Xcom: 특정 dag, 특정 schedule에 수행되는 Task 간에만 공유
# 로컬 airflow에서 admin -> Variable 에서 등록해서 사용 (db에 등록됨)

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    #스케줄러의 주기적 DAG 파시싱시 Variable.get 갯수만큼 DB연결을 이르켜 불필요한 부하 발생, 스케줄러 과부하 원인 중 하나
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    # Jinja 탬플릿(권고) 형식으로 사용
    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )