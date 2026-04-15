import json
import time

# 현재 워킹디렉토리에서 코드를 작동할때 경로
from log_generator import LogGenerator

log_gen = LogGenerator()
log_gen_map = {
  'finance': log_gen.finance,
  'factory': log_gen.factory,
}


def make_log(config):
  print(f'{config["target_industry"]} 로그 생성 시작')
  print('-' * 50)
  for i in range(50):
    # log = log_gen.finance()
    cur_func = log_gen_map.get(config['target_industry'])
    log = cur_func()
    # ensure_ascii 이스케이프 문자들 변환 없이 있는 그대로 출력하는가?
    log_json = json.dumps(log, ensure_ascii=False)
    print(f'[Log-{i + 1}] {log_json}')
    # 대기시간 (다음 로그 발생까지), 동시간에 n개로그 x
    # 다른시간대에 1개의 로그 형태임 -> 컨셉 따라 다르게 구성 가능
    time.sleep(log_gen.get_interval_time(config['mode'], config['interval']))
    # break
  print('-' * 50)


if __name__ == '__main__':
  config = {
    'target_industry': 'finance',
    'mode': 'random',
    'interval': 1,
    'total_count': 10,
    'loop': False,
  }
  make_log(config)
