from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

# Connection : UI화면에서 등록한 커넥션 정보
# Hook의 개념 : Airflow에서 외부 솔루션의 기능을 사용할 수 있도록 미리 구현된 클래스, 커넥션 정보를 통해 생성되는 객체

with DAG(
        dag_id='dags_python_with_postgres_hook',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        
        # psycopg2.connect()를 이용해서 직접 값을 넣는 것이 아니라 
        # PostgresHook()으로 Hook 객체를 호출 -> 보안문제 없이 정보를 획득할 수 있음
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id='insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )
    insrt_postgres_with_hook