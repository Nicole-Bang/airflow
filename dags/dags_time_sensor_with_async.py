import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

# DateTimeSensorAsync와 같이 뒤에 Async가 붙은 오퍼레이터는 Deferrable Operator라 부르며 Triggerer에게 작업 완료 수신을 맡기는 오퍼레이터라는 의미

with DAG(
    dag_id="dags_time_sensor_with_async",
    start_date=pendulum.datetime(2023, 5, 1, 0, 0, 0),
    end_date=pendulum.datetime(2023, 5, 1, 1, 0, 0),
    schedule="*/10 * * * *",    # 10분에 1번씩
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )