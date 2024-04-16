from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

# import BaseHook을 했을 때, a = BaseHook()으로 객체화하여 사용하는 경우가 일반적이나
# @classmethod 데코레이션을 사용시 이런 객체화를 사용하지 않아도 된다.

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)   #classMethod 유형 -> BaseHook.get_connection(self.postgres_conn_id)로 다이렉트 접근 가능
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        # psycopg2.connect -> db와의 연결 
        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn   # airflow_conn와는 다른것임을 인지해야함

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:                             
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue 
                
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )