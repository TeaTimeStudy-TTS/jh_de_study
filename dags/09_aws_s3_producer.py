'''
- 목표
    - 데이터 생산(etl등 통해서) -> CSV -> s3 업로드(PUSH) 처리
    - 배치 작업( 특정 시간대에 스케줄링하여 일괄 처리 ) -> airflow 목표
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 환경변수 설정
# 버킷명 (iam계정-827913617635-리전-an)
BUCKET_NAME = "de-ai-01-827913617635-ap-northeast-2-an"
# 업로드할 파일명 준비
FILE_NAME = "sensor_data.csv"
# 업로드할 파일의 로컬내 위치 -> 컨테이너 기반
S3_KEY = f"income/{FILE_NAME}"
# 업로드할 로컬 파일의 위치 (컨테이너 -> 리눅스 기반)
LOCAL_PATH = f"/opt/airflow/dags/data/{FILE_NAME}"

with DAG(
  dag_id="09_aws_s3_producer",
  description="aws 연동, s3 업로드",
  default_args={
    "owner": "de_2team_manager",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
  },
  schedule_interval=None, # 스케쥴 x -> 트리거 작동으로 실행
  start_date=datetime(2026, 2, 25),
  catchup=False,
  tags=["aws", "s3", "producer"],
) as dag:
  task_create_dummy_data_csv = PythonOperator(
    task_id="task_create_dummy_data_csv",
    bash_command = f'echo "id,timestamp,value\n1, $(date),100\n2,$(date),500" > {LOCAL_PATH}'
  )
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id="upload_to_s3",
    filename=LOCAL_PATH,  # 로컬PC등 원본 리소스의 위치(파일명 포함)
    dest_key=S3_KEY,  # s3 특정 버킷내에 FILE_NAME으로 저장(생성)
    dest_bucket=BUCKET_NAME,  # 버킷 네임
    aws_conn_id="aws_default",  # aws 접속 정보
    replace=True,  # 동일 파일이 있으면 덮는다
  )
  
  pass