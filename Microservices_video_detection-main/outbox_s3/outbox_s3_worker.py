"""
Outbox S3 Worker — реализация паттерна Transactional Outbox для сохранения JSON в S3

НОВАЯ АРХИТЕКТУРА:
1. Опрашивает MongoDB runner_db.runner_predictions на записи со статусом "predictable"
2. При нахождении такой записи, читает prediction JSON из runner_db.runner_predictions
3. Сохраняет prediction JSON в MinIO (S3) по пути detection/{event_id}.json
4. Обновляет статус в outbox на "predicted" и сохраняет путь в output_s3
5. НЕ удаляет запись из runner_predictions (остаётся для истории и отладки)

Даёт надёжное сохранение в S3, даже если MinIO временно недоступен.
Гарантирует, что все результаты детекции будут сохранены в S3.
Поддерживает микросервисную архитектуру с разделением БД.
"""
from pymongo import MongoClient
from minio import Minio
import json
import time
import io
import os
from datetime import datetime

# Подключение к MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")
RUNNER_MONGO_URI = os.getenv("RUNNER_MONGO_URI", "mongodb://admin:password@mongo:27017/")
client = MongoClient(MONGO_URI)
db = client['main_db']
outbox = db['outbox']

# Подключение к runner_db для получения результатов
runner_client = MongoClient(RUNNER_MONGO_URI)
runner_db = runner_client['runner_db']
runner_predictions = runner_db['runner_predictions']

# Подключение к MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False

# Ждём готовности MinIO
minio_client = None
while True:
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        print("[outbox_s3] Connected to MinIO!", flush=True)
        break
    except Exception as e:
        print(f"[outbox_s3] MinIO not ready yet, retrying... {e}", flush=True)
        time.sleep(5)

# Основной цикл: ищем записи со статусом "predictable" в runner_db и сохраняем в S3
while True:
    try:
        # Ищем записи напрямую в runner_db со статусом "predictable" (финальный JSON готов)
        ready_predictions = list(runner_predictions.find({"status": "predictable"}).limit(10))
        
        if ready_predictions:
            print(f"[outbox_s3] Found {len(ready_predictions)} events with 'predictable' status in runner_db to save to S3", flush=True)
        
        for runner_pred in ready_predictions:
            event_id = runner_pred["_id"]
            bucket = runner_pred.get("bucket", "videos")
            prediction = runner_pred.get("prediction")
            
            if not prediction:
                print(f"[outbox_s3] No prediction found in runner_db for event_id={event_id}, skipping", flush=True)
                continue
            
            try:
                # Сохраняем prediction в S3 (MinIO)
                detection_key = f"detection/{event_id}.json"
                json_bytes = json.dumps(prediction, ensure_ascii=False, indent=2).encode("utf-8")
                
                minio_client.put_object(
                    bucket,
                    detection_key,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type="application/json"
                )
                print(f"[outbox_s3] Saved prediction JSON to S3: {bucket}/{detection_key}", flush=True)
                
                # Обновляем статус в outbox на "predicted" и сохраняем путь к файлу
                predicted_at = datetime.utcnow()
                outbox.update_one(
                    {"_id": event_id},
                    {
                        "$set": {
                            "status": "predicted",
                            "predicted_at": predicted_at,
                            "output_s3": {
                                "bucket": bucket,
                                "object": detection_key
                            }
                        }
                    }
                )
                
                # НЕ удаляем prediction из runner_predictions (оставляем для истории и отладки)
                # В новой архитектуре данные остаются в runner_db
                print(f"[outbox_s3] Status updated to 'predicted' for event_id={event_id}, prediction kept in runner_db", flush=True)
                
            except Exception as e:
                print(f"[outbox_s3] Failed to save prediction to S3 for {event_id}: {e}", flush=True)
                # Запись в runner_db останется со статусом "predictable" и будет обработана при следующей итерации
    except Exception as e:
        print(f"[outbox_s3] Worker error: {e}", flush=True)
    
    # Проверяем каждую секунду
    time.sleep(1)
