import json
import multiprocessing
from confluent_kafka import Consumer, TopicPartition
from datetime import datetime
from pymongo import MongoClient

# MongoDB Writer 프로세스
def mongo_writer(write_queue: multiprocessing.Queue):
    mongo_client = MongoClient("mongodb://localhost:27017/")
    collection = mongo_client["sensor_database"]["sensor_data"]
    print("📝 MongoDB Writer 프로세스 시작됨")

    buffer = []

    while True:
        try:
            # 데이터가 queue에 들어올 때까지 기다림 (timeout 가능)
            try:
                data = write_queue.get()
            except queue.Full:
                print("⚠️ 큐가 가득 찼습니다. 잠시 대기합니다...", flush=True)

            # 종료 신호
            if data == "STOP":
                break

            buffer.append(data)

            # 일정량 쌓이면 한 번에 저장
            if len(buffer) >= 1000:
                collection.insert_many(buffer)
                print(f"📝 MongoDB 저장 완료: {len(buffer)}건", flush=True)
                buffer.clear()

        except Exception as e:
            print(f"🚨 MongoDB 저장 오류: {e}", flush=True)

    # 종료 시 남은 버퍼 정리
    if buffer:
        collection.insert_many(buffer)
    print("🧹 MongoDB Writer 종료")

# ✅ Kafka Consumer 프로세스
def kafka_consumer_worker(partition_id: int, write_queue: multiprocessing.Queue):
    print(f"🛰 Kafka Consumer 프로세스 시작: 파티션 {partition_id}")

    # Kafka Consumer 설정
    conf = {
        "bootstrap.servers": "localhost:9092",             # Kafka 브로커 주소
        "group.id": f"sensor-group-{partition_id}",        # Consumer group ID (파티션별 유니크)
        "auto.offset.reset": "earliest",                   # 오프셋 없을 경우 가장 처음부터 읽기
        "enable.auto.commit": False                        # 수동 커밋 (정확한 메시지 처리 보장)
    }

    consumer = Consumer(conf)
    consumer.assign([TopicPartition("sensor_data", partition_id)]) # 파티션 지정하여 구독

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Kafka 에러: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8")) # Kafka 메시지를 JSON 파싱
                data["timestamp"] = datetime.fromisoformat(data["timestamp"]) # timestamp 필드를 문자열에서 datetime으로 변환

                write_queue.put(data)  # ✅ 큐에 데이터 넣기
                
                consumer.commit(msg) # 메시지 커밋
            except Exception as e:
                print(f"🚨 메시지 처리 실패: {e}")

    finally:
        consumer.close()

# ✅ 메인
if __name__ == "__main__":
    process_count = 10  # Kafka 파티션 수 = Consumer 수
    write_queue = multiprocessing.Queue(maxsize=10000) # MongoDB에 쓸 데이터 큐 (최대 10,000건)

    # MongoDB 쓰기 전용 프로세스 시작
    writer_proc = multiprocessing.Process(target=mongo_writer, args=(write_queue,))
    writer_proc.start()

    # Kafka Consumer 프로세스 시작
    consumers = []
    for pid in range(process_count):
        proc = multiprocessing.Process(target=kafka_consumer_worker, args=(pid, write_queue))
        proc.start()
        consumers.append(proc)

    try:
        # 메인 프로세스가 종료될 때까지 대기
        for proc in consumers:
            proc.join()
    except KeyboardInterrupt:
        print("🛑 종료 신호 감지됨")

    # ✅ Writer에게 종료 신호 보내기
    write_queue.put("STOP")
    writer_proc.join()

    print("✅ 전체 종료 완료")
