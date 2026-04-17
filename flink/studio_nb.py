%flink.pyflink

# st_env => 자체적으로 준비 되어 있는 객체 사용
'''
setting = EnvironmentSettings.new_instance().in_streaming_mode().build()
# SQL과 유사한 방식으로 데이터를 다룰수 있는 객체
st_env = TableEnvironment.create( setting )
'''

# 1. 입력 테이블 정의
t_env.execute_sql("""
    create table stock_input (
            ticker STRING,
            price DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
        ) with (
            "connector" = "kinesis",
            "stream"    = "de-ai-01-an2-kds-stock-input",
            "aws.region"= "ap-northeast-2",
            "scan.stream.initpos" = "LATEST",
            "format"    = "json"
        )
        """)

# 2. 출ㄹ 테이블 정의
t_env.execute_sql("""
        create table stock_output (
            ticker STRING,
            avg_price DOUBLE,
            avg_time TIMESTAMP(3)
        ) with (
            "connector" = "kinesis",
            "stream"    = "de-ai-01-an2-kds-stock-output",
            "aws.region"= "ap-northeast-2",            
            "format"    = "json"
        )
    """)

# 3. 연산처리 (10초 단위로 데이터를 그룹화, 평균집계)
t_env.execute_sql("""
        INSERT INTO stock_output
        SELECT
            ticker,
            AVG(price) as avg_price,
            TUMBLE_END(event_time, INTERVAL '10' SECOND)  as avg_time
        from
            stock_input
        GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), ticker
                      
    """)