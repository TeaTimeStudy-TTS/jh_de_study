'''
- 요구사항
  - 신용평가(예측)만 담당하는 API 구성
  - AI 모델이 처리하는 것 처럼 전체 틀만 구성, 실제는 간단한 공식 처리
  - API
    - 요청 데이터
      - 1 ~ n명 데이터 => [개별개인정보(dict), ...]
    - 응답 데이터
      - 1 ~ n명 데이터 => [개별개인정보(dict), ...]
'''

# 1. 모듈 가져오기
from fastapi import FastAPI    # 앱 자체 의미
from pydantic import BaseModel # 해당 클레스를 상속 -> 요청/응답 데이터 구조 정의
from typing import List        # 요청 데이터에 대한 형태 정의(n개 데이터에 대한 타입 표현) -> 유효성 점검용
import random # 신용평가시 활용
# 2. FastAPI 앱(객체) 생성
app = FastAPI()


# 3. 요청/응답 데이터의 구조를 정의하는 + 유효성검사 틀을 제공하는 클레스 구성 -> pydantic
class ReqData(BaseModel): 
  user_id:str
  income:int
  loan_amt:int
  pass

class ResData(BaseModel):
  user_id:str
  credit_score:int
  grade:str
  pass

# 4. 라우팅 (url 정의, 해당 요청시 처리할 함수 매칭)
# @app -> 데커레이터 -> 함수 안에 함수가 존재하는 2중  구조 -> 특정함수에 공통 기능 부여시 유용
# 웹프로그램에서 자주 보임 (요청을 전달 하는 기능 공통등...)
@app.get('/') # URL 정의, http프로토콜의 method를 정의(get 방식)
def home():
  return {'status' : 'AI 신용평가 서비스 API'}