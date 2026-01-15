"""
Outbox S3 Worker — реализация паттерна Transactional Outbox для сохранения JSON в S3

Архитектура:
1. Опрашивает MongoDB runner_db.runner_predictions на записи со статусом "predictable"
2. При нахождении такой записи, читает prediction JSON из runner_db.runner_predictions
3. Сохраняет prediction JSON в MinIO (S3) по пути detection/{event_id}.json
4. Обновляет статус в runner_db на "ready" и сохраняет путь в output_s3
5. НЕ удаляет запись из runner_predictions (остаётся для истории и отладки)

Даёт надёжное сохранение в S3, даже если MinIO временно недоступен.
Гарантирует, что все результаты детекции будут сохранены в S3.
Поддерживает микросервисную архитектуру с разделением БД.
Статус "ready" в runner_db означает, что результат готов к забору из S3.

Оптимизации:
- Полностью асинхронный код (async/await)
- Параллельная обработка записей
- Идемпотентность и отказоустойчивость
"""
import asyncio
import io
import json
import os
import traceback
from datetime import datetime

from minio import Minio
from pymongo import MongoClient

RUNNER_MONGO_URI = os.getenv("RUNNER_MONGO_URI", "mongodb://admin:password@mongo:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False


def get_runner_client():
    return MongoClient(RUNNER_MONGO_URI)


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


async def wait_for_minio():
    loop = asyncio.get_running_loop()
    while True:
        try:
            minio_client = await loop.run_in_executor(None, get_minio_client)
            await loop.run_in_executor(None, minio_client.list_buckets)
            print("[outbox_s3] Connected to MinIO!", flush=True)
            return minio_client
        except Exception as e:
            print(f"[outbox_s3] MinIO not ready yet, retrying... {e}", flush=True)
            await asyncio.sleep(5)


async def process_prediction_async(runner_pred, minio_client, runner_predictions):
    event_id = runner_pred["_id"]
    bucket = runner_pred.get("bucket", "videos")
    prediction = runner_pred.get("prediction")
    detection_key = f"detection/{event_id}.json"

    if not prediction:
        print(
            f"[outbox_s3] No prediction found in runner_db for event_id={event_id}, "
            f"skipping",
            flush=True
        )
        return

    loop = asyncio.get_running_loop()

    def check_status():
        current = runner_predictions.find_one({"_id": event_id})
        return current and current.get("status") == "predictable"

    is_predictable = await loop.run_in_executor(None, check_status)
    if not is_predictable:
        print(
            f"[outbox_s3] Event {event_id} was already processed by another worker, "
            f"skipping",
            flush=True
        )
        return

    try:
        def check_file_exists():
            try:
                minio_client.stat_object(bucket, detection_key)
                return True
            except Exception:
                return False

        file_exists = await loop.run_in_executor(None, check_file_exists)

        if file_exists:
            print(
                f"[outbox_s3] File already exists in S3: {bucket}/{detection_key} "
                f"(idempotency)",
                flush=True
            )
        else:
            def save_to_s3():
                json_bytes = json.dumps(prediction, ensure_ascii=False, indent=2).encode("utf-8")
                minio_client.put_object(
                    bucket,
                    detection_key,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type="application/json"
                )

            await loop.run_in_executor(None, save_to_s3)
            print(
                f"[outbox_s3] Saved prediction JSON to S3: {bucket}/{detection_key}",
                flush=True
            )

        def update_status():
            ready_at = datetime.utcnow()
            result = runner_predictions.update_one(
                {"_id": event_id, "status": "predictable"}, 
                {
                    "$set": {
                        "status": "ready",
                        "ready_at": ready_at,
                        "output_s3": {
                            "bucket": bucket,
                            "object": detection_key
                        }
                    }
                }
            )
            return result.modified_count

        modified_count = await loop.run_in_executor(None, update_status)

        if modified_count == 0:
            print(
                f"[outbox_s3] Status was already updated for event_id={event_id} "
                f"(idempotency check)",
                flush=True
            )
        else:
            print(
                f"[outbox_s3] Status updated to 'ready' in runner_db for event_id={event_id}, "
                f"prediction kept in runner_db",
                flush=True
            )

    except Exception as e:
        print(
            f"[outbox_s3] Failed to save prediction to S3 for {event_id}: {e}",
            flush=True
        )
        raise


async def process_outbox_s3_loop():
    loop = asyncio.get_running_loop()
    
    runner_client = await loop.run_in_executor(None, get_runner_client)
    runner_db = runner_client['runner_db']
    runner_predictions = runner_db['runner_predictions']

    minio_client = await wait_for_minio()

    try:
        while True:
            try:
                def get_predictions():
                    return list(runner_predictions.find({"status": "predictable"}).limit(10))

                loop = asyncio.get_running_loop()
                ready_predictions = await loop.run_in_executor(None, get_predictions)

                if ready_predictions:
                    print(
                        f"[outbox_s3] Found {len(ready_predictions)} events with "
                        f"'predictable' status in runner_db to save to S3",
                        flush=True
                    )

                    tasks = [
                        process_prediction_async(runner_pred, minio_client, runner_predictions)
                        for runner_pred in ready_predictions
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for runner_pred, result in zip(ready_predictions, results):
                        if isinstance(result, Exception):
                            print(
                                f"[outbox_s3] Error processing event "
                                f"{runner_pred['_id']}: {result}",
                                flush=True
                            )

            except Exception as e:
                print(f"[outbox_s3] Worker error: {e}", flush=True)
                traceback.print_exc()

            await asyncio.sleep(1)

    finally:
        runner_client.close()


async def main():
    print("[outbox_s3] Starting async outbox S3 worker...", flush=True)
    await process_outbox_s3_loop()


if __name__ == "__main__":
    asyncio.run(main())