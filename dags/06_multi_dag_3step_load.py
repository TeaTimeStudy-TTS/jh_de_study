from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import random
import pandas as pd
import os

DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _load(**kwargs):
    # csv => df => mysql 적제
    # 1. csv 경로 획득 -> xcom을 통해서 이전 task(게시자)의 id를 이용하여 추출 <- ti 필요
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='trasform')

    # 2. csv -> df (도입 근거 => 소규모 데이터이므로)
    df = pd.read_csv(csv_path)

    # 3. mysql 연결 => MySqlHook 사용
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득 -> I/o 영향 있음(예외처리등 필요, with문)
    # 7. 전체를 try ~ except로 감싸기(I/O)
    # 실제는 실패 작업인데, 성공으로 오인할수 있다 -> 예외 던지기 필요함
    try:
        # 4. 커서를 획득하여 
        with conn.cursor() as cursor:        
            # 4-1. insert 구문 사용
            sql = '''
                insert into sensor_readings
                (sensor_id, timestamp, temperature_c, temperature_f)
                values (%s, %s, %s, %s)
            '''            
            # 여러 데이터를 한번에 넣을때 유용 => executemany() 대응
            params = [
                ( data['sensor_id'],     data['timestamp'], 
                  data['temperature'], data['temperature_f'] )
                for _, data in df.iterrows() # 데이터가 없을때까지 반복함 -> 데이터가 한세트씩 추출됨
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany( sql, params ) # 한번에 밀어 넣기
            # 4-2. 커밋
            conn.commit()
            logging.info('mysql에 적제 완료')
            pass        
    except Exception as e:
        logging.info(f'적제 오류 : {e}') # 예외 던지기로 변경 필요(리뷰때 시도 )
        pass
    finally:
        # 5. 연결 종료
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료 (뒷정리)')
    
    pass

with DAG(
    dag_id      = "06_multi_dag_3step_load", 
    description = "load 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['load', 'etl'],
) as dag:
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )