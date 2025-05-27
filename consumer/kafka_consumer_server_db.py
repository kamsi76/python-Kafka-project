# 📌 kafka_consumer_server.py

import json
import threading
from fastapi import FastAPI
from confluent_kafka import Consumer
import hashlib
import os
from datetime import datetime
from pymongo import MongoClient

app = FastAPI()

# MongoDB 설정
mong_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mong_client["sensor_database"]
mongo_collection = mongo_db["sensor_data"]

# Kafka Consumer 설정
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "sensor-group",               # 고정 그룹으로 커밋 유지
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False  # 수동 커밋
}

consumer = Consumer(conf)
consumer.subscribe(["sensor_data"])

# ===== 로그 저장 디렉토리 =====
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

buffer = []  # 메시지 버퍼
def get_log_file_path():
    """오늘 날짜 기준 로그 파일 경로 반환"""
    today = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(LOG_DIR, f"{today}.jsonl")

def write_to_file(data: dict):
    """센서 데이터를 JSONL 형식으로 파일에 저장"""
    try:
        file_path = get_log_file_path()
        with open(file_path, 'a', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')
    except Exception as e:
        print(f"❌ 파일 저장 실패: {e}")

def consume_loop():
    print("🛰 Kafka Consumer 스레드 시작됨...")

    try:
        while True:
            message = consumer.poll(0.1)
            if message is None:
                continue
            if message.error():
                print(f"❌ Kafka Error: {message.error()}")
                continue

            try:
                data = json.loads(message.value().decode('utf-8'))
                data["timestamp"] = datetime.fromisoformat(data.get("timestamp", "unknown"))
                buffer.append(data)

                # ✅ 파일 저장
                # write_to_file(data)

                if len(buffer) >= 1000:  # 버퍼가 1000개 이상이면
                    mongo_collection.insert_many(buffer)  # 버퍼에 쌓인 데이터 일괄 저장    
                    buffer.clear()  # 버퍼 초기화
                
                # # ✅ 수신 로그 + 중복 방지 해시 추가
                # # ✅ 중복 확인을 위한 메시지 SHA256 해시 출력
                # raw_bytes = message.value()
                # digest = hashlib.sha256(raw_bytes).hexdigest()
                # print(f"{sensor_id} → {value} | SHA256: {digest}")

                # ✅ 메시지 커밋 (정상 처리 후)
                consumer.commit(message=message)

            except Exception as e:
                print(f"🚨 메시지 파싱 실패: {e}")

    except KeyboardInterrupt:
        print("🛑 Kafka Consumer 중단")
    finally:
        consumer.close()
        print("🧹 Kafka Consumer 정상 종료")

@app.on_event("startup")
def startup_kafka_consumer():
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()
    print("✅ Kafka Consumer 백그라운드 스레드 시작됨")

@app.get("/")
def root():
    return {"message": "Kafka Consumer → MongoDB 저장 서버 작동 중!"}
