# sensor_emulator_process.py
import json, time, random
from kafka import KafkaProducer
from datetime import datetime
from multiprocessing import Process

sensor_ids = [f"sensor_{i}" for i in range(10)]
records_per_sec = 3200
topic_name = "sensor_data"

def generate_sensor_data(sensor_id):
    # 안정성 강화된 KafkaProducer 설정
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        batch_size=32 * 1024,
        compression_type='gzip',
        acks='all',
        enable_idempotence=True,
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

    print(f"센서 {sensor_id} 시작 (초당 {records_per_sec} 건 전송)")

    while True:
        start_time = time.time()
        total_sent = 0

        for _ in range(records_per_sec):
            is_anomaly = random.random() < 0.05
            value = (
                random.choice([random.uniform(500, 10000), random.uniform(-10000, -500)])
                if is_anomaly else round(random.uniform(10.0, 100.0), 2)
            )

            data = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now().isoformat(),
                "value": round(value, 2)
            }

            producer.send(topic_name, value=data)
            total_sent += 1

        print(f"[{sensor_id}] 전송 시간: {time.time() - start_time:.3f}s / {total_sent}건", flush=True)
        time.sleep(max(0, 1 - (time.time() - start_time)))

def main():
    processes = [Process(target=generate_sensor_data, args=(sid,)) for sid in sensor_ids]
    for p in processes: p.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 종료 요청 감지됨")

if __name__ == "__main__":
    main()
