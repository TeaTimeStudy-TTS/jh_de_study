"""
- DAG 스케줄은 하루에 한번(00시00분00초) 지정하뒤 -> 테스트는 트리거 작동
- T1 : S3에 특정위치에 적제된 데이터를 기반으로 테이블 구성
    - ~/csvs/ 하위 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석결과를 담은 테이블 삭제(존재하면)
    - daily_report_tbl 삭제 쿼리 수행(존재하면)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석결과를 담은 테이블에 연결 -> 결과레포트용 데이터 구성
    - 시험 결과를 기반으로, 결과, 카운트, 평균, 최소, 최대 -> 그룹화 수행(기준 result)
    - 테이블명 => daily_report_tbl
        - format = 'PARQUET'
        - external_location = '원하는 s3 위치로 지정' -> 쿼리 결과가 저장되는 곳
    - output_location = '원하는 s3 위치로 지정', -> 테이블의 메타 정보가 저장되는 곳
- 미구현 -> T3 데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의종성 : T1 >> T2 >> T3
"""

# 1. 모듈가져오기
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import (
  S3DeleteObjectsOperator,
  S3CreateObjectOperator,
)

# 2. 환경변수
BUCKET_NAME = 'de-ai-01-827913617635-ap-northeast-2-an'
ATHENA_DB_NAME = 'de-ai-01-an2-glue-db'
SRC_TABLE = 's3_exam_csv_tbl'
TARGET_TABLE = 'daily_report_tbl'

# 메타 정보, 임시 정보 필요시 저장/삭제 공간으로 활용
S3_SOURCE_LOC = f's3://{BUCKET_NAME}/csvs/'
S3_TARGET_LOC = f's3://{BUCKET_NAME}/athena/result/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의
with DAG(
  dag_id='10_aws_athena_query',
  description='athena query 작업',
  default_args={
    'owner': 'de_2team_manager',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
  },
  schedule_interval='@daily', 
  start_date=datetime(2026, 2, 25),
  catchup=False,
  tags=['aws', 's3', 'athena', 'query'],
) as dag:
  # 4. task 정의

  query1 = f"""
    CREATE EXTERNAL TABLE if not exists `{SRC_TABLE}`(
        `id` int,
        `name` string,
        `score` int,
        `created_at` string,
        `result` string
      )
      ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
      STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      LOCATION
        '{S3_SOURCE_LOC}'
      TBLPROPERTIES (
        'skip.header.line.count'='1'
)
  """

  t1 = AthenaOperator(
    task_id='create_s3_target',  # 작업 ID
    query=query1,
    database=ATHENA_DB_NAME,
    output_location=S3_QUERY_LOG_LOC,
    aws_conn_id='aws_default',  # 접속 정보
  )
  # 임시로 사용한 테이블 삭제 -> 클린
  t2 = AthenaOperator(
    task_id='drop_table',
    query=f'drop table if exists {TARGET_TABLE}',
    database=ATHENA_DB_NAME,
    output_location=S3_QUERY_LOG_LOC,  # 쿼리 수행 결과 로그 저장 위치
    aws_conn_id='aws_default',  # 접속 정보
  )
  query2 = f"""
        create table {TARGET_TABLE}
        with (
            format = 'PARQUET', 
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        as
        
        select
            result
            count(*) as row_count,
            avg(score) as avg_score,
            min(score) as min_score,
            max(score) as max_score
        from {SRC_TABLE} 
        group by result
    """
  t3 = AthenaOperator(
    task_id='create_table_format_parquet',
    query=query2,
    database=ATHENA_DB_NAME,
    output_location=S3_QUERY_LOG_LOC,
    aws_conn_id='aws_default'
  )

  # 5. 의존성 구성
  t1 >> t2 >> t3
  pass
