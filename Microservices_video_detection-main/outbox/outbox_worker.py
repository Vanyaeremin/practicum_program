"""
Outbox Worker — реализация паттерна Outbox для публикации событий

Паттерн transactional outbox:
1. Опрашивает MongoDB outbox на события со статусом "new"
2. Публикует их в Kafka тему "outbox_events"
3. Ставит статус "sent" после успешной отправки

Даёт надёжную доставку событий, даже если сервисы временно недоступны.
Гарантирует, что события, созданные при загрузке видео, дойдут до обработки.
"""
from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time

# Подключаемся к MongoDB
client = MongoClient("mongodb://admin:password@mongo:27017/")
db = client['main_db']
outbox = db['outbox']

# Ждём готовности Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
        )
        print("Connected to Kafka!")
        break
    except Exception as e:
        print("Kafka not ready yet, retrying...", e)
        time.sleep(5)

# Основной цикл: ищем новые события и публикуем в Kafka
while True:
    # Ищем события со статусом "new" (созданы в api_checkout)
    events = list(outbox.find({"status": "new"}).limit(10))
    print(f"Found {len(events)} new event(s) in outbox")
    
    for event in events:
        message = {
            "_id": str(event["_id"]),
            "payload": event["payload"],  # bucket, object (путь к видео), media_type
            "created_at": str(event.get("created_at")),
        }
        try:
            # Публикуем в Kafka тему "outbox_events" (читает runner)
            producer.send("outbox_events", message)
            print(f"Sent to Kafka: {message}")
            # Отмечаем событие как "sent"
            outbox.update_one(
                {"_id": event["_id"]},
                {"$set": {"status": "sent"}}
            )
        except Exception as e:
            print(f"Failed to send to Kafka: {e}")
    
    # Проверяем новые события каждую секунду
    time.sleep(1)
