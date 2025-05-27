from kafka import KafkaProducer
import json
import time
import random
import threading
from datetime import datetime

# kafka producer 설정
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # Kafka 브로커 주소
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # JSON 직렬화
)

sensor_ids = [f"sensor_{i}" for i in range(10)]  # 10개의 센서 ID 생성
records_per_sec = 3200  # 초당 전송할 레코드 수
topic_name = "sensor_data"  # Kafka 토픽 이름

def generate_sensor_data(sensor_id):
    """
    센서 데이터를 주기적으로 생성하고 Kafka로 전송
    """
    print(f"센서 {sensor_id} 시작 (초당 {records_per_sec} 건 전송)")

    while True:
        start_time = time.time()  # 초당 전송 시간 측정

        sended_records = 0  # 전송된 레코드 수 초기화
        for _ in range(records_per_sec):
           
            is_anomaly = random.random() < 0.05  # 이상치 여부 결정 (5% 확률)

            if is_anomaly:
                # 이상치 생성: 너무 높은 값 또는 너무 낮은 값
                value = random.choice([
                    random.uniform(500, 10000),   # 너무 높은 값
                    random.uniform(-10000, -500)  # 너무 낮은 값
                ])
            else:
                value = round(random.uniform(10.0, 100.0), 2) # (10.0, 100.0) 범위의 값 생성

            data = {
                "sensor_id": sensor_id, # 센서 ID
                "timestamp": datetime.now().isoformat(), # 현재 시간 ISO 포맷
                "value": value # 센서 값
            }

            # print(f"센서 {sensor_id} 데이터 전송: {data}")

            # Kafka에 데이터 전송 (value 파라미터 명시!)
            producer.send(topic_name, value=data)
            sended_records += 1

        # 🧹 1초에 한 번 flush (버퍼 전송)
        producer.flush()

        print(f"센서 {sensor_id} 전송 건수: {sended_records}")

        # 초당 records_per_sec 건을 맞추기 위한 대기 시간
        elapsed = time.time() - start_time
        time.sleep(max(0, 1 - elapsed))

threads = []

# 각 센서 ID에 대해 스레드 생성 및 시작
for sensor_id in sensor_ids:
    thread = threading.Thread(target=generate_sensor_data, args=(sensor_id,), daemon=True)  # 데몬 스레드로 설정    
    thread.start()
    threads.append(thread)

# 메인 스레드가 종료되지 않도록 대기
try:
    while True:
        time.sleep(1)  # 메인 스레드가 계속 실행되도록 대기
except KeyboardInterrupt:
    print("에뮬레이터종료!!!")
    producer.close()  # Kafka 프로듀서 종료