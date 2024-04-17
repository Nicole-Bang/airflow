from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
import pendulum
from datetime import timedelta
from airflow.models import Variable

# email 관련 파라미터
# email: str | Iterable[str] | None = None,     -> email 파라미터만 입력하면 email_on_failure 파라미터는 True로 자동설정됨
# email_on_retry: bool = conf.getboolean("email", "default_email_on_retry", fallback=True),         -> 재시도 했을 때, 전송
# email_on_failure: bool = conf.getboolean("email", "default_email_on_failure", fallback=True),     -> 실패 했을 때, 전송

email_str = Variable.get("email_target")        # airflow Variables에 등록해둠
email_lst = [email.strip() for email in email_str.split(',')] # ,로 구분한 값을 잘라서 list로 저장

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True, 
        'email': email_lst
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('에러 발생') # 에러발생을 유도해 둠
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',      # 에러발생을 유도해 둠(exit 0 이 아니면 다 에러)
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )

    # >>를(플로우) 안주면 모두 병렬로 수행 