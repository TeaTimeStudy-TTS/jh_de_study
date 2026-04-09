'''
  - 함수 내부 연산의 결과에 의해 조건부로 task를 선택하여 진행 (의존성 컨트롤)

'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule 
import logging

# 2. DAG 정의

  # 3. task 정의
  
  # 4. 의존성 정의
