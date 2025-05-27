from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import threading

sensor_ids = ["sensor1", "sensor2", "sensor3"]
records_per_sec = 3200
topic_name = "sensor_data"

# ✅ 전역 변수 선언만 하고, 실제 초기화는 main()에서!
producer = None

def generate_sensor_data(sensor_id):
    """
    센서 데이터를 주기적으로 생성하고 Kafka로 전송
    """
    global producer  # 💡 외부에서 초기화된 전역 producer 객체 사용

    print(f"센서 {sensor_id} 시작 (초당 {records_per_sec} 건 전송)")

    while True:
        start_time = time.time()

        for _ in range(records_per_sec):
            # 이상치 여부 결정 (5% 확률)
            is_anomaly = random.random() < 0.05

            if is_anomaly:
                value = random.choice([
                    random.uniform(500, 10000),   # 너무 높은 값
                    random.uniform(-10000, -500)  # 너무 낮은 값
                ])
            else:
                value = round(random.uniform(10.0, 100.0), 2)

            # 센서 데이터 생성
            data = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now().isoformat(),
                "value": round(value, 2)
            }

            # Kafka에 데이터 전송 (value 파라미터 명시!)
            producer.send(topic_name, value=data)

        # 🧹 1초에 한 번 flush (버퍼 전송)
        producer.flush()

        # 초당 records_per_sec 건을 맞추기 위한 대기 시간
        elapsed = time.time() - start_time
        time.sleep(max(0, 1 - elapsed))

def main():
    global producer

    # ✅ KafkaProducer를 한 번만 생성
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        batch_size=32 * 1024,
        compression_type='gzip',
        acks=1,
        retries=3,
    )

    # 센서별 스레드 실행
    threads = []
    for sensor_id in sensor_ids:
        thread = threading.Thread(target=generate_sensor_data, args=(sensor_id,))
        thread.start()
        threads.append(thread)

    # 💤 메인 스레드는 종료 방지용
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 프로그램 종료 요청")
        producer.close()

if __name__ == "__main__":
    main()
