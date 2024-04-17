import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id="dags_time_sensor",
    start_date=pendulum.datetime(2023, 5, 1, 0, 0, 0),
    end_date=pendulum.datetime(2023, 5, 1, 1, 0, 0),
    schedule="*/10 * * * *",    # 10분마다 1번씩 돈다
    catchup=True,
) as dag:
    # DateTimeSenso : 목표로 하는 시간까지 기다리는 sensor
    sync_sensor = DateTimeSensor(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )