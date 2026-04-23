from opensearchpy import OpenSearch
from datetime import datetime
import random
import time

# 2. 환경변수
from dotenv import load_dotenv
import os

load_dotenv()

HOST = os.getenv('OPENSEARCH_HOST')
AUTH = (os.getenv('AUTH_NAME'), os.getenv('AUTH_PW'))

client = OpenSearch(
  hosts=[{'host': HOST, 'port': 443}],  # https -> 443
  http_auth=AUTH,
  http_compress=True,
  use_ssl=True,
  verify_certs=True,
  ssl_assert_hostname=False,
  ssl_show_warn=False,
)

# 4. index 생성 ->
index_name = 'factory-45-sensor-v1'
if not client.indices.exists(index=index_name):  # 인덱스가 없는가?
  client.indices.create(index=index_name)

# 5. 데이터 생성 -> 전송
# 공장에 장비(오븐) 3대, 데이터를 1초마다 전송
oven_ids = ['OVEN-001', 'OVEN-002', 'OVEN-003']
while True:
  for oven_id in oven_ids:
    temp = random.uniform(200, 220)  # 정상범위
    if random.random() > 0.95:
      temp += random.uniform(30, 50)

    # json(반정형) 형태의 데이터 구성
    doc = {
      'timestamp': datetime.now(),
      'oven_id': oven_id,
      'temperature': round(temp, 2),
      'vibration': round(random.uniform(0, 0.15), 2),
      'status': 'DANGER' if temp >= 240 else 'Normal',
    }
    # pass
    response = client.index(index=index_name, body=doc, refresh=True)
    print(f'{oven_id} 온도: {doc["temperature"]}')
  time.sleep(1)
