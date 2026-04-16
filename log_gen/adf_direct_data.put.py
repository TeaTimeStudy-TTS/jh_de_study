"""
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플

"""

import boto3
import json
import time
from run import make_one_log

# 2. 환경변수
ACCESS_KEY = ''
SECRET_KEY = ''
REGION = 'ap-northeast-2'


# 3. 특정 서비스(ADF) 클라이언트 생성
# AWS 외부에서 진행
def get_client(service_name='firehose', is_in_aws=True):
  if not is_in_aws:
    session = boto3.Session(
      aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION
    )
    return session.client('firehose')

  # AWS 내부에서 진행
  return boto3.client('firehose', region_name=REGION)


firehose = get_client()

# 4. 로그 생성 및 ADF 발송


def send_log():
  res = firehose.put_record(
    DeliveryStreamName='de-ai-01-an2-kdf-log-to-s3',
    Record={'Data': make_one_log() + '\n'},
  )

  print(f'전송결과 {res}')


for i in range(10):
  send_log()
