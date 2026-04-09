"""
- macro + jinja 활용하여 airflow 내부 정보 접근

"""

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

def _print(**kwargs):
  logging.info(f'ds 출력 { kwargs["ds"] }')
  logging.info(f'ds 출력 { kwargs["ds_nodash"] }')
  pass


# 2. DAG 정의
with DAG(
  dag_id="03_basics_context_jinja",  # 최소로 구성된 필수 옵션
  description="macro를 통해 context 접근, jinja를 통해 표현",
  default_args={
    "owner": "de_2team_manager",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
  },
  schedule_interval="0 9 * * *",  # 분 시 ... -> 매일 오저 09시 00분에 스케줄 작동
  start_date=datetime(2026, 2, 25),
  catchup=False,
  tags=["jinja", "xcom", "context"],
) as dag:
  # 3. task 정의 (operator를 활용)
  t1 = BashOperator(
    task_id="jinja_used_bash",
    # jinja에서 값 출력 -> {{변수|값|식|함수등등}}
    # {{context의 키값 }}
    bash_command="echo 'DAG 수행시간 {{ ds }}, {{ ds_nodash }}' ",
  )
  t2 = BashOperator(
    task_id="jinja_macro_bash", 
    bash_command = "echo '일주일전 수행 시간 계산 {{ macros.ds_add(ds, -7) }}, 랜덤 {{ macros.random() }}' "
  )
  t3 = PythonOperator(
    task_id="jinja_used_python", 
    python_callable = _print
  )

  # 4. 의존성 정의(task 실행 방향성 설정)
  t1 >> t2 >> t3
  pass
