# 📌 kafka_consumer_multi.py
import json
import multiprocessing
from confluent_kafka import Consumer, TopicPartition
from pymongo import MongoClient
from datetime import datetime

def process_partition(partition_id):

    # Kafka Consumer 설정: 프로세스마다 고유한 consumer group을 지정
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"sensor-group-{partition_id}",  # 프로세스별 고유 그룹
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    # 각 프로세스는 자신이 담당할 하나의 파티션만 구독
    consumer.assign([TopicPartition("sensor_data", partition_id)])

    print(f"✅ Consumer 프로세스 시작 (파티션 {partition_id})")

    # MongoDB 클라이언트 및 컬렉션 초기화
    mongo_client = MongoClient("mongodb://localhost:27017/")
    collection = mongo_client["sensor_database"]["sensor_data"]

    # 데이터를 일정량 버퍼링해서 bulk insert
    buffer = []

    try:
        while True:
            msg = consumer.poll(0.1)    # Kafka로부터 메시지를 100ms 간격으로 폴링
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Kafka Error: {msg.error()}")
                continue

            try:

                data = json.loads(msg.value().decode('utf-8')) # 메시지 디코딩
                data["timestamp"] = datetime.fromisoformat(data["timestamp"]) # ISO 포맷 문자열을 datetime 객체로 변환
                buffer.append(data) # 버퍼에 추가

                # 일정량이 모이면 MongoDB에 한번에 저장
                if len(buffer) >= 500:
                    collection.insert_many(buffer)
                    print(f"📦 파티션 {partition_id} - {len(buffer)}건 데이터 저장")
                    buffer.clear()

                consumer.commit(msg) # 메시지 커밋
            except Exception as e:
                print(f"🚨 메시지 처리 오류: {e}")
    finally:
        consumer.close()
        mongo_client.close()

if __name__ == "__main__":
    processes = []
    for partition in range(10):  # 파티션 수만큼 프로세스 생성
        p = multiprocessing.Process(target=process_partition, args=(partition,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
