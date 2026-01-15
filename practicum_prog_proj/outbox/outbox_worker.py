"""
Outbox Worker — реализация паттерна Outbox для публикации событий

Паттерн transactional outbox:
1. Опрашивает MongoDB outbox на события со статусом "new"
2. Публикует их в Kafka тему "outbox_events"
3. Ставит статус "sent" после успешной отправки

Даёт надёжную доставку событий, даже если сервисы временно недоступны.
Гарантирует, что события, созданные при загрузке видео, дойдут до обработки.
"""
import asyncio
import json
import os
import traceback

from aiokafka import AIOKafkaProducer
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def get_mongo_client():
    return MongoClient(MONGO_URI)


async def wait_for_kafka():
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                enable_idempotence=True,
                acks='all', 
            )
            await producer.start()
            print(
                f"[outbox] Connected to Kafka with idempotent producer! "
                f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}",
                flush=True
            )
            print(
                "[outbox] Producer initialized successfully",
                flush=True
            )
            return producer
        except Exception as e:
            print(f"[outbox] Kafka not ready yet, retrying... {e}", flush=True)
            await asyncio.sleep(5)


async def process_event_async(event, producer, outbox):
    loop = asyncio.get_running_loop()

    def check_status():
        current = outbox.find_one({"_id": event["_id"]})
        return current and current.get("status") == "new"

    is_new = await loop.run_in_executor(None, check_status)
    if not is_new:
        print(
            f"[outbox] Event {event['_id']} was already processed "
            f"by another worker, skipping",
            flush=True
        )
        return

    message = {
        "_id": str(event["_id"]),
        "payload": event["payload"],
        "created_at": str(event.get("created_at")),
    }

    try:
        event_id_str = str(event["_id"])
        print(
            f"[outbox] Attempting to send event {event_id_str} to Kafka...",
            flush=True
        )

        try:
            result = await producer.send_and_wait(
                "outbox_events",
                value=message,
                key=event_id_str
            )
            print(
                f"[outbox] Successfully sent to Kafka: event_id={event_id_str}, "
                f"topic={result.topic}, partition={result.partition}, "
                f"offset={result.offset}",
                flush=True
            )
        except (AttributeError, TypeError) as e:
            print(
                f"[outbox] send_and_wait not available, using alternative method: {e}",
                flush=True
            )
            future = producer.send(
                "outbox_events",
                value=message,
                key=event_id_str
            )
            result = await future
            await producer.flush()
            print(
                f"[outbox] Successfully sent to Kafka (alternative method): "
                f"event_id={event_id_str}, topic={result.topic}, "
                f"partition={result.partition}, offset={result.offset}",
                flush=True
            )

        def update_status():
            result = outbox.update_one(
                {"_id": event["_id"], "status": "new"},
                {"$set": {"status": "sent"}}
            )
            return result.modified_count

        modified_count = await loop.run_in_executor(None, update_status)

        if modified_count == 0:
            print(
                f"[outbox] Event {event['_id']} was already processed "
                f"(idempotency check)",
                flush=True
            )
        else:
            print(
                f"[outbox] Status updated to 'sent' for event {event['_id']}",
                flush=True
            )

    except Exception as e:
        print(
            f"[outbox] Failed to send to Kafka for event "
            f"{event.get('_id', 'unknown')}: {e}",
            flush=True
        )
        traceback.print_exc()
        raise


async def process_outbox_loop():
    loop = asyncio.get_running_loop()
    
    client = await loop.run_in_executor(None, get_mongo_client)
    db = client['main_db']
    outbox = db['outbox']

    producer = await wait_for_kafka()

    try:
        while True:
            try:
                def get_events():
                    return list(outbox.find({"status": "new"}).limit(10))

                loop = asyncio.get_running_loop()
                events = await loop.run_in_executor(None, get_events)

                if events:
                    print(f"[outbox] Found {len(events)} new event(s) in outbox", flush=True)

                    tasks = [process_event_async(event, producer, outbox) for event in events]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for event, result in zip(events, results):
                        if isinstance(result, Exception):
                            print(
                                f"[outbox] Error processing event {event['_id']}: {result}",
                                flush=True
                            )
                            traceback.print_exc()

            except Exception as e:
                print(f"[outbox] Worker error: {e}", flush=True)
                traceback.print_exc()

            await asyncio.sleep(1)

    finally:
        await producer.stop()
        client.close()


async def main():
    print("[outbox] Starting async outbox worker...", flush=True)
    await process_outbox_loop()

if __name__ == "__main__":
    asyncio.run(main())