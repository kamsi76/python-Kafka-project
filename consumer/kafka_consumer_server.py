from fastapi import FastAPI
from confluent_kafka import Consumer, KafkaException
import threading
from pymongo import MongoClient as MongClient  # MongoDB 클라이언트 임포트
import json  # JSON 처리 모듈

app = FastAPI() # FastAPI 인스턴스 생성

mongo_client = MongClient("mongodb://localhost:27017/")  # MongoDB 클라이언트 생성
mongo_db = mongo_client["sensor_database"]  # MongoDB 데이터베이스 설정
mongo_collection = mongo_db["sensor_data"]  # MongoDB 컬렉션 설정

config = {
    "bootstrap.servers": "localhost:9092",  # Kafka 브로커 주소
    "group.id": "sensor-group",  # 고정 그룹으로 커밋 유지
    "auto.offset.reset": "earliest",  # 가장 오래된 메시지부터 읽기
    "enable.auto.commit": True  # 자동 커밋
}

consumer = Consumer(config)  # Kafka Consumer 인스턴스 생성
consumer.subscribe(["sensor_data"])  # Kafka 토픽 구독

def consume_messages():
    """
    Kafka 메시지를 소비하는 함수
    """
    print("🛰 Kafka Consumer 스레드 시작됨...")

    try:
        while True:
            message = consumer.poll(1.0)  # 1초 대기
            if message is None:
                continue  # 메시지가 없으면 계속 대기
            if message.error():
                raise KafkaException(message.error())  # 에러 발생 시 예외 처리

            # 메시지 처리 로직
            data = json.loads( message.value().decode('utf-8'))  # 메시지 디코딩

            # MongoDB에 데이터 저장
            mongo_collection.insert_one(data)
            
            # print(f"수신된 메시지: {data}")  # 수신된 메시지 출력

    except KeyboardInterrupt:
        print("Consumer 중단됨")
        pass  # 키보드 인터럽트 시 종료
    finally:
        consumer.close()  # Consumer 종료

@app.on_event("startup")
def startup_event():
    """
    FastAPI 애플리케이션 시작 시 Consumer 스레드 시작
    """
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    print("Kafka Consumer 스레드가 시작되었습니다.")

@app.get("/")
def read_root():
    """
    기본 엔드포인트
    """
    return {"message": "Kafka Consumer 서버가 실행 중입니다!"}