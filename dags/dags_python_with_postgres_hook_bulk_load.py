from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        dag_id='dags_python_with_postgres_hook_bulk_load',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus_bulk1',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )

    # Bulk_load의 문제점
    # Load 가능한 Delimiter는 tab으로 고정되어 있음
    # Header까지 포함해서 업로드 됨
    # 특수문자로 인해 파싱이 안될 경우 에러 발생

    # 개선방안
    # Custom Hook을 만들어서 Delimiter 유형을 입력받게 하고
    # Header 여부를 선택하게끔 하며
    # 특수문자를 제거하는 로직을 추가 후
    # sqlalchemy를 이용하여 Load 한다면? 그리고 테이블을 생성하면서 업로드 한다면??