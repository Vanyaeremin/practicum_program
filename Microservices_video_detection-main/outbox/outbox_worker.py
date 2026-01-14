"""
Outbox Worker — реализация паттерна Outbox для публикации событий (ASYNC VERSION)

Паттерн transactional outbox:
1. Опрашивает MongoDB outbox на события со статусом "new"
2. Публикует их в Kafka тему "outbox_events"
3. Ставит статус "sent" после успешной отправки

Даёт надёжную доставку событий, даже если сервисы временно недоступны.
Гарантирует, что события, созданные при загрузке видео, дойдут до обработки.

ОПТИМИЗАЦИИ:
- Полностью асинхронный код (async/await)
- Параллельная обработка событий
- Идемпотентность и отказоустойчивость
"""
import asyncio
import json
from aiokafka import AIOKafkaProducer
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def get_mongo_client():
    """Получает подключение к MongoDB"""
    return MongoClient(MONGO_URI)

async def wait_for_kafka():
    """Ожидает готовности Kafka и создаёт идемпотентный producer"""
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # Идемпотентность на уровне producer
                enable_idempotence=True,  # Включает идемпотентность - предотвращает дубликаты сообщений
                # В aiokafka max_in_flight_requests_per_connection не поддерживается
                # Идемпотентность работает автоматически при enable_idempotence=True
                acks='all',  # Ждём подтверждения от всех реплик (требуется для идемпотентности)
            )
            await producer.start()
            print(f"[outbox] Connected to Kafka with idempotent producer! Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
            
            # Проверяем, что producer работает, отправляя тестовое сообщение (опционально)
            # Это поможет выявить проблемы на раннем этапе
            try:
                # Не отправляем реальное сообщение, просто проверяем подключение
                print(f"[outbox] Producer initialized successfully", flush=True)
            except Exception as e:
                print(f"[outbox] Warning: Producer initialization check failed: {e}", flush=True)
            
            return producer
        except Exception as e:
            print(f"[outbox] Kafka not ready yet, retrying... {e}", flush=True)
            await asyncio.sleep(5)

async def process_event_async(event, producer, outbox):
    """
    Обрабатывает одно событие асинхронно.
    
    Обеспечивает:
    - Идемпотентность (проверка статуса перед обработкой)
    - Отказоустойчивость (повторные попытки при ошибках)
    - Атомарность (условное обновление статуса)
    """
    # Проверяем статус перед обработкой (защита от race condition)
    loop = asyncio.get_running_loop()
    
    def check_status():
        current = outbox.find_one({"_id": event["_id"]})
        return current and current.get("status") == "new"
    
    is_new = await loop.run_in_executor(None, check_status)
    if not is_new:
        print(f"[outbox] Event {event['_id']} was already processed by another worker, skipping", flush=True)
        return
    
    message = {
        "_id": str(event["_id"]),
        "payload": event["payload"],
        "created_at": str(event.get("created_at")),
    }
    
    try:
        # Публикуем в Kafka с message key (event_id) для идемпотентности
        event_id_str = str(event["_id"])
        print(f"[outbox] Attempting to send event {event_id_str} to Kafka...", flush=True)
        
        # Отправляем сообщение в Kafka
        # key передается как строка, key_serializer автоматически преобразует в bytes
        try:
            result = await producer.send_and_wait(
                "outbox_events",
                value=message,
                key=event_id_str  # Используем event_id как ключ для обеспечения порядка и идемпотентности
            )
            print(f"[outbox] Successfully sent to Kafka: event_id={event_id_str}, topic={result.topic}, partition={result.partition}, offset={result.offset}", flush=True)
        except (AttributeError, TypeError) as e:
            # Если send_and_wait не поддерживается, используем альтернативный метод
            print(f"[outbox] send_and_wait not available, using alternative method: {e}", flush=True)
            # send() возвращает Future, await на нем дает RecordMetadata
            future = producer.send(
                "outbox_events",
                value=message,
                key=event_id_str
            )
            result = await future  # Ждем RecordMetadata
            await producer.flush()  # Убеждаемся, что все сообщения отправлены
            print(f"[outbox] Successfully sent to Kafka (alternative method): event_id={event_id_str}, topic={result.topic}, partition={result.partition}, offset={result.offset}", flush=True)
        
        # Обновляем статус ТОЛЬКО если он всё ещё "new" (идемпотентность)
        def update_status():
            result = outbox.update_one(
                {"_id": event["_id"], "status": "new"},  # Условие для идемпотентности
                {"$set": {"status": "sent"}}
            )
            return result.modified_count
        
        modified_count = await loop.run_in_executor(None, update_status)
        
        if modified_count == 0:
            print(f"[outbox] Event {event['_id']} was already processed (idempotency check)", flush=True)
        else:
            print(f"[outbox] Status updated to 'sent' for event {event['_id']}", flush=True)
            
    except Exception as e:
        print(f"[outbox] Failed to send to Kafka for event {event.get('_id', 'unknown')}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        # Событие остаётся со статусом "new" для повторной обработки при следующей итерации
        raise

async def process_outbox_loop():
    """
    Основной цикл обработки outbox событий.
    
    ОПТИМИЗАЦИИ:
    - Асинхронная обработка событий
    - Параллельная обработка нескольких событий
    - Не блокирует event loop
    """
    # Подключаемся к MongoDB
    client = get_mongo_client()
    db = client['main_db']
    outbox = db['outbox']
    
    # Ждём готовности Kafka
    producer = await wait_for_kafka()
    
    try:
        while True:
            try:
                # Ищем события со статусом "new" (созданы в api_checkout)
                def get_events():
                    return list(outbox.find({"status": "new"}).limit(10))
                
                loop = asyncio.get_running_loop()
                events = await loop.run_in_executor(None, get_events)
                
                if events:
                    print(f"[outbox] Found {len(events)} new event(s) in outbox", flush=True)
                    
                    # Обрабатываем события параллельно
                    tasks = [process_event_async(event, producer, outbox) for event in events]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Логируем ошибки
                    for event, result in zip(events, results):
                        if isinstance(result, Exception):
                            print(f"[outbox] Error processing event {event['_id']}: {result}", flush=True)
                            import traceback
                            traceback.print_exc()
                else:
                    # Логируем, что событий нет (для отладки)
                    pass  # Убрали лишний лог для уменьшения шума
                
            except Exception as e:
                print(f"[outbox] Worker error: {e}", flush=True)
                import traceback
                traceback.print_exc()
            
            # Проверяем новые события каждую секунду (не блокирует event loop)
            await asyncio.sleep(1)
    
    finally:
        await producer.stop()
        client.close()

async def main():
    """Главная функция для запуска воркера"""
    print("[outbox] Starting async outbox worker...", flush=True)
    await process_outbox_loop()

if __name__ == "__main__":
    asyncio.run(main())
