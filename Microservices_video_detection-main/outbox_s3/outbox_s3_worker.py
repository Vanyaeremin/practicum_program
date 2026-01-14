"""
Outbox S3 Worker — реализация паттерна Transactional Outbox для сохранения JSON в S3 (ASYNC VERSION)

АРХИТЕКТУРА:
1. Опрашивает MongoDB runner_db.runner_predictions на записи со статусом "predictable"
2. При нахождении такой записи, читает prediction JSON из runner_db.runner_predictions
3. Сохраняет prediction JSON в MinIO (S3) по пути detection/{event_id}.json
4. Обновляет статус в runner_db на "ready" и сохраняет путь в output_s3
5. НЕ удаляет запись из runner_predictions (остаётся для истории и отладки)
6. НЕ обновляет outbox - это соответствует принципам микросервисов

Даёт надёжное сохранение в S3, даже если MinIO временно недоступен.
Гарантирует, что все результаты детекции будут сохранены в S3.
Поддерживает микросервисную архитектуру с разделением БД.
Статус "ready" в runner_db означает, что результат готов к забору из S3.

ОПТИМИЗАЦИИ:
- Полностью асинхронный код (async/await)
- Параллельная обработка записей
- Идемпотентность и отказоустойчивость
"""
import asyncio
import json
import io
from pymongo import MongoClient
from minio import Minio
from datetime import datetime
import os

RUNNER_MONGO_URI = os.getenv("RUNNER_MONGO_URI", "mongodb://admin:password@mongo:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False

def get_runner_client():
    """Получает подключение к runner_db"""
    return MongoClient(RUNNER_MONGO_URI)

def get_minio_client():
    """Получает подключение к MinIO"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

async def wait_for_minio():
    """Ожидает готовности MinIO"""
    while True:
        try:
            minio_client = get_minio_client()
            # Проверяем доступность через list_buckets
            minio_client.list_buckets()
            print("[outbox_s3] Connected to MinIO!", flush=True)
            return minio_client
        except Exception as e:
            print(f"[outbox_s3] MinIO not ready yet, retrying... {e}", flush=True)
            await asyncio.sleep(5)

async def process_prediction_async(runner_pred, minio_client, runner_predictions):
    """
    Обрабатывает одну запись prediction асинхронно.
    
    Обеспечивает:
    - Идемпотентность (проверка статуса и существования файла)
    - Отказоустойчивость (повторные попытки при ошибках)
    - Атомарность (условное обновление статуса)
    """
    event_id = runner_pred["_id"]
    bucket = runner_pred.get("bucket", "videos")
    prediction = runner_pred.get("prediction")
    detection_key = f"detection/{event_id}.json"
    
    if not prediction:
        print(f"[outbox_s3] No prediction found in runner_db for event_id={event_id}, skipping", flush=True)
        return
    
    loop = asyncio.get_running_loop()
    
    # Проверяем статус перед обработкой (защита от race condition)
    def check_status():
        current = runner_predictions.find_one({"_id": event_id})
        return current and current.get("status") == "predictable"
    
    is_predictable = await loop.run_in_executor(None, check_status)
    if not is_predictable:
        print(f"[outbox_s3] Event {event_id} was already processed by another worker, skipping", flush=True)
        return
    
    try:
        # Проверяем, не существует ли уже файл в S3 (идемпотентность)
        def check_file_exists():
            try:
                minio_client.stat_object(bucket, detection_key)
                return True
            except Exception:
                return False
        
        file_exists = await loop.run_in_executor(None, check_file_exists)
        
        if file_exists:
            print(f"[outbox_s3] File already exists in S3: {bucket}/{detection_key} (idempotency)", flush=True)
        else:
            # Сохраняем prediction в S3 (MinIO)
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
            print(f"[outbox_s3] Saved prediction JSON to S3: {bucket}/{detection_key}", flush=True)
        
        # Обновляем статус ТОЛЬКО если он всё ещё "predictable" (идемпотентность)
        def update_status():
            ready_at = datetime.utcnow()
            result = runner_predictions.update_one(
                {"_id": event_id, "status": "predictable"},  # Условие для идемпотентности
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
            print(f"[outbox_s3] Status was already updated for event_id={event_id} (idempotency check)", flush=True)
        else:
            print(f"[outbox_s3] Status updated to 'ready' in runner_db for event_id={event_id}, prediction kept in runner_db", flush=True)
            
    except Exception as e:
        print(f"[outbox_s3] Failed to save prediction to S3 for {event_id}: {e}", flush=True)
        # Запись в runner_db останется со статусом "predictable" и будет обработана при следующей итерации
        raise

async def process_outbox_s3_loop():
    """
    Основной цикл обработки outbox S3 событий.
    
    ОПТИМИЗАЦИИ:
    - Асинхронная обработка записей
    - Параллельная обработка нескольких записей
    - Не блокирует event loop
    """
    # Подключаемся к MongoDB
    runner_client = get_runner_client()
    runner_db = runner_client['runner_db']
    runner_predictions = runner_db['runner_predictions']
    
    # Ждём готовности MinIO
    minio_client = await wait_for_minio()
    
    try:
        while True:
            try:
                # Ищем записи напрямую в runner_db со статусом "predictable" (финальный JSON готов)
                def get_predictions():
                    return list(runner_predictions.find({"status": "predictable"}).limit(10))
                
                loop = asyncio.get_running_loop()
                ready_predictions = await loop.run_in_executor(None, get_predictions)
                
                if ready_predictions:
                    print(f"[outbox_s3] Found {len(ready_predictions)} events with 'predictable' status in runner_db to save to S3", flush=True)
                    
                    # Обрабатываем записи параллельно
                    tasks = [
                        process_prediction_async(runner_pred, minio_client, runner_predictions)
                        for runner_pred in ready_predictions
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Логируем ошибки
                    for runner_pred, result in zip(ready_predictions, results):
                        if isinstance(result, Exception):
                            print(f"[outbox_s3] Error processing event {runner_pred['_id']}: {result}", flush=True)
                
            except Exception as e:
                print(f"[outbox_s3] Worker error: {e}", flush=True)
                import traceback
                traceback.print_exc()
            
            # Проверяем каждую секунду (не блокирует event loop)
            await asyncio.sleep(1)
    
    finally:
        runner_client.close()

async def main():
    """Главная функция для запуска воркера"""
    print("[outbox_s3] Starting async outbox S3 worker...", flush=True)
    await process_outbox_s3_loop()

if __name__ == "__main__":
    asyncio.run(main())
