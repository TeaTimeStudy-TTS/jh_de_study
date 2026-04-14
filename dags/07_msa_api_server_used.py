'''
  API 호출 과정 적용, 데이터 처리에 대한 스케줄 구성
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import requests # api 호출용, MSA 서비스 호출용

# 2. API 서버 주소
API_URL = 'http://127.0.0.1:8000/predict'

def _create_dummy_data(**kwargs):
  users = [
    {"user_id":"C001","income":5000, "loan_amt":2000},
    {"user_id":"C002","income":4000, "loan_amt":5000},
    {"user_id":"C003","income":8000, "loan_amt":1000}
  ]
  return users

def _create_dummy_data(**kwargs):
  # 1. 이전 task의 결과물 획득 (차후 -> 데이터레이크 혹은 )
  ti = kwargs['ti']
  users_data = ti.xcom_pull(task_ids="task_create_dummy_data")
  
  # 2. 신용 평가 요청 응답 -> api 호출 (차후 LLM 모델과 연계 가능) -> 통신 -> I/O -> 예외처리
  try:
    # 3. post 방식으로 요청, dict 형태 데이터 첨부 -> json 형태로 전달 (내부적으로는 객체 직렬화 처리됨)
    res = requests.post( API_URL, json=users_data)
    # if res.raise_for_status() == 200
    # 5. 결과 획득 (객체의 역직렬화 : json 형태 문자열 => dict 혹은 list[ dict, ...] 형태)
    results = res.json()
    # 6. 결과 출력 
    logging.info(f'신용 평가 결과 획득 {results}')
    
  except Exception as e :
    logging.error(f'호출실패: {e}')
    raise
  
  
  pass

def _create_dummy_data(**kwargs):
  pass

# 3. DAG 정의
with DAG(
  
) as dag:
  # 4. Task 정의
  # 4-1. 더미 데이터 준비 -> 추후 고객 정보 저장 -> 추후 s3 업로드
  task_create_dummy_data = PythonOperator(
    task_create_dummy_data_id = "",
  )
  # 4-2. API 호출(AI 서비스 활용) -> 신용평가 획득
  task_api_service_call = PythonOperator()
  # 4-3. 결과저장 -> 추후 고객 정보 업데이트
  task_load_users_credit = PythonOperator()
  
  # 5. 의존성, 각 task는 xCom 통신으로 데이터 공유
  task_create_dummy_data >> task_api_service_call >> task_load_users_credit
  