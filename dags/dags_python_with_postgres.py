from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2     #파이썬에서 sql을 수행할 수 있게 해줌
        from contextlib import closing
        
        # psycopg2.connect() : DB서버와의 연결(session)
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            # closing : conn.close()
            # cursor : 실제 sql을 실행할 수 있게 해주는 역할
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg)) # 바인딩 할 값을 맵핑
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'hjkim', 'hjkim', 'hjkim']
    )
        
    insrt_postgres

    # 이 코드의 문제점
    # 접속정보 노출 : postgres DB에 대한 user, password 등
    # 접송정보 변경시 대응 어려움

    # 해결 방법
    # variable 이용 (variable 등록 필요)
    # Hook 이용 (variable 등록 필요 없음)