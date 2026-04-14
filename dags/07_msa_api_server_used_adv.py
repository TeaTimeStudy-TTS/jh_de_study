"""
  기존 07.. DAG을 업그레이드(발전된 버전)
  고객 데이터는 사전에 디비에 입력해둠(신용평가 부분만 제외)
  고객 데이터는 조회하여 평가 요청으로 변경
  신용 평가 내용을 update sql을 통해서 반영
"""

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


API_URL = "http://ai-api-server:8000/predict"


def _create_dummy_data(**kwargs):
  users = [
    {"user_id": "C001", "income": 5000, "loan_amt": 2000},
    {"user_id": "C002", "income": 4000, "loan_amt": 5000},
    {"user_id": "C003", "income": 8000, "loan_amt": 1000},
  ]
  return users

def _extract_data(**kwargs):
  pass

def _api_service_call(**kwargs):
  ti = kwargs["ti"]
  users_data = ti.xcom_pull(task_ids="task_create_dummy_data")

  try:
    res = requests.post(API_URL, json=users_data)
    results = res.json()
    logging.info(f"credit result {results}")

  except Exception as e:
    logging.error(f"api call fail: {e}")
    raise

  return results


def _load_users_credit(**kwargs):
  # 1. 신용 평가 결과가 획득
  ti = kwargs["ti"]
  users_grade = ti.xcom_pull(task_ids="task_api_service_call")
  if not users_grade:
    logging.error("신용 평가 결과 없음")
    raise ValueError("신용 평가 결과 없음")
  # 2. MySqlHook를 이용하여 연결
  mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
  with mysql_hook.get_conn() as conn:
    with conn.cursor() as cursor:
      cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            user_id VARCHAR(50) PRIMARY KEY,
            income INT DEFAULT NULL,
            loan_amt INT DEFAULT NULL,
            credit_score INT DEFAULT NULL,
            grade VARCHAR(10) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
      )
    # 4. 신용평가 결과 삽입(추후 고객 정보 업데이트로 조정)
    sql = """
        insert into customers
        (user_id, credit_score, grade )
        values
        (%s, %s, %s)
    """
    params = [
      (data["user_id"], data["credit_score"], data["grade"]) for data in users_grade
    ]
    cursor.executemany(sql, params)
    # 5. 커밋
    conn.commit()
    # 6. 연결종료
    pass

  pass


with DAG(
  dag_id="07_msa_api_server_used",
  description="msa 환경에 특정 서비스 ai 스위치 컨셉)을 호출하여 신용평가를 수행하는 다중직",
  default_args={
    "owner": "de_2team_manager",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
  },
  schedule_interval="@daily",
  start_date=datetime(2026, 2, 25),
  catchup=False,
  tags=["msa", "fastapi"],
) as dag:
  # 4-0. 더미 데이터 준비 -> DB에 직접 입력(편의상 구성)
  task_create_dummy_data = PythonOperator(
    task_id="task_create_dummy_data", python_callable=_create_dummy_data
  )
  
  # 4-1. 고객 데이터 획득 (extract) -> select 쿼리 조회
  task_extract_data = PythonOperator(
    task_id = "task_extract_data",
    python_callable= _extract_data
  )

  task_api_service_call = PythonOperator(
    task_id="task_api_service_call", python_callable=_api_service_call
  )

  task_load_users_credit = PythonOperator(
    task_id="task_load_users_credit", python_callable=_load_users_credit
  )

task_create_dummy_data >> task_api_service_call >> task_load_users_credit
