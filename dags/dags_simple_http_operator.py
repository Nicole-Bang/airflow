# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

# SimpleHttpOperator: http 요청을 하고 결과로 text를 리턴받는 오퍼레이터
# Xcom에 저장된다

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',     # 커넥션정보
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',  #전역변수 variable을 이용한다
        # {{var.value.apikey_openapi_seoul_go_kr}}를(airflow의 variable에 등록) 사용할때의 장점
        # api를 이용하는 모든 dag에서 이용이 가능하다
        # String으로 작성하는 것 보다는 보안에 도움이 된다
        method='GET', #Xcom에 저장됨
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt)) # Json형태로 보여준다
        
    tb_cycle_station_info >> python_2()