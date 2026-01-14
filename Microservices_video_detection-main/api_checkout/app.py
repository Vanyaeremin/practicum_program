"""
API Checkout Service — входная точка для загрузки видео и получения результатов

Сервис отвечает за:
1. Загрузку видео через эндпоинт /upload_video
2. Сохранение видео в объектное хранилище MinIO
3. Создание событий в outbox для асинхронной обработки (статусы: "new", "sent")
4. Получение результатов детекции (проверяет наличие JSON в S3)

УПРОЩЁННАЯ АРХИТЕКТУРА СТАТУСОВ:
- API НЕ обращается к runner_db
- API проверяет наличие JSON файла в S3 (detection/{event_id}.json)
- Если файл есть в S3 - статус "ready" (готов к забору)
- Если файла нет в S3 - статус из outbox ("new" или "sent")
- Outbox (main_db.outbox): только "new" и "sent" (для outbox паттерна)
"""
from fastapi import FastAPI, UploadFile, File, HTTPException
import httpx
from pymongo import MongoClient
from datetime import datetime
import uuid
import os
from minio import Minio

app = FastAPI()

@app.get("/api/")
@app.get("/api")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "service": "api_checkout"}

@app.get("/api/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "service": "api_checkout"}

# Настройки MinIO (S3-совместимое хранилище) для хранения видео
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False

# Инициализация MinIO клиента (ленивая - при первом использовании)
minio_client = None

def get_minio_client():
    """Получить MinIO клиент, создавая его при необходимости"""
    global minio_client
    if minio_client is None:
        try:
            print(f"[MinIO] Initializing client for {MINIO_ENDPOINT}", flush=True)
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            print(f"[MinIO] Client initialized successfully", flush=True)
        except Exception as e:
            print(f"[MinIO] Error initializing client: {e}", flush=True)
            raise
    return minio_client

@app.post("/api/request")
async def request_to_outbox(payload: dict):
    import asyncio
    loop = asyncio.get_event_loop()

    def mongo_insert():
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        event = {
            "_id": str(uuid.uuid4()),
            "payload": payload,
            "created_at": datetime.utcnow(),
            "status": "new"
        }
        outbox.insert_one(event)
        client.close()
        return event["_id"]

    event_id = await loop.run_in_executor(None, mongo_insert)
    return {"status": "ok", "event_id": event_id}


from fastapi import status

@app.post("/api/upload_video")
async def upload_and_enqueue_video(
    file: UploadFile = File(...),
    bucket: str = "videos"
):
    import asyncio
    import io

    loop = asyncio.get_event_loop()

    try:
        video_id = str(uuid.uuid4())
        filename = f"input/{video_id}.mp4"
        data = await file.read()

        print(f"[upload_video] Received file: {file.filename}, size: {len(data)} bytes", flush=True)

        # --- MinIO: bucket check + upload ---
        def minio_upload():
            client = get_minio_client()

            if not client.bucket_exists(bucket):
                print(f"[upload_video] Creating bucket: {bucket}", flush=True)
                client.make_bucket(bucket)

            print(f"[upload_video] Uploading to MinIO: {bucket}/{filename}", flush=True)
            client.put_object(
                bucket,
                filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type="video/mp4"
            )

        # --- MongoDB: outbox event ---
        def mongo_insert():
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']
            outbox = db['outbox']
            event = {
                "_id": video_id,
                "payload": {
                    "bucket": bucket,
                    "object": filename,
                    "media_type": "video"
                },
                "created_at": datetime.utcnow(),
                "status": "new"
            }
            outbox.insert_one(event)
            client.close()

        # ОПТИМИЗАЦИЯ: Параллельная загрузка в MinIO и MongoDB для ускорения
        # Обе операции выполняются одновременно, что уменьшает общее время обработки
        try:
            minio_task = loop.run_in_executor(None, minio_upload)
            mongo_task = loop.run_in_executor(None, mongo_insert)
            
            # Ждём завершения обеих операций параллельно
            await asyncio.gather(minio_task, mongo_task)
            
            print(f"[upload_video] Successfully uploaded to MinIO and created outbox event: {video_id}", flush=True)
        except Exception as error:
            # Определяем тип ошибки для более точного сообщения
            error_msg = str(error)
            if "MinIO" in error_msg or "minio" in error_msg.lower() or "bucket" in error_msg.lower():
                print(f"[upload_video] Error uploading to MinIO: {error}", flush=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"MinIO upload error: {error_msg}"
                )
            else:
                print(f"[upload_video] Error creating outbox event: {error}", flush=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"MongoDB event creation error: {error_msg}"
                )

        return {"status": "ok", "event_id": video_id}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[upload_video] Unexpected exception: {e}", flush=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error: {str(e)}"
        )

# Словарь с пояснениями статусов на русском языке
# УПРОЩЁННАЯ АРХИТЕКТУРА СТАТУСОВ:
# - API проверяет наличие JSON файла в S3 (detection/{event_id}.json)
# - Если файл есть в S3 - статус "ready" (готов к забору)
# - Если файла нет в S3 - статус из outbox ("new" или "sent")
STATUS_DESCRIPTIONS = {
    "new": "Новое событие создано, ожидает отправки в очередь обработки",
    "sent": "Событие отправлено в очередь обработки",
    "ready": "Результат сохранён в S3 и готов к забору"
}

@app.get("/api/status/all")
async def get_all_statuses():
    """
    Получить статусы всех операций (ID Видео - код статуса - расшифровка)
    
    УПРОЩЁННАЯ АРХИТЕКТУРА:
    - Читает события из outbox
    - Для каждого события проверяет наличие JSON файла в S3 (detection/{event_id}.json)
    - Если файл есть в S3 - статус "ready" (готов к забору)
    - Если файла нет в S3 - статус из outbox ("new" или "sent")
    
    Возвращает:
    - video_id: идентификатор видео
    - status_code: код статуса (new, sent, ready)
    - status_description: расшифровка статуса на русском языке
    """
    import asyncio
    
    try:
        loop = asyncio.get_event_loop()
        
        def get_outbox_events():
            """Читает события из outbox"""
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']
            outbox = db['outbox']
            events = list(outbox.find({}, {"_id": 1, "status": 1}))
            client.close()
            return events
        
        outbox_events = await loop.run_in_executor(None, get_outbox_events)
        
        # Функция для проверки наличия файла в S3
        def check_s3_file_exists(bucket, object_name):
            """Проверяет наличие файла в S3"""
            try:
                minio_cli = get_minio_client()
                # Пытаемся получить метаданные объекта (не скачиваем сам файл)
                minio_cli.stat_object(bucket, object_name)
                return True
            except Exception:
                return False
        
        # Формируем результат
        result = []
        
        for event in outbox_events:
            video_id = str(event.get("_id"))
            outbox_status = event.get("status", "unknown")
            
            # Проверяем наличие JSON файла в S3
            detection_key = f"detection/{video_id}.json"
            bucket = "videos"  # По умолчанию
            
            file_exists = await loop.run_in_executor(
                None, 
                check_s3_file_exists, 
                bucket, 
                detection_key
            )
            
            # Определяем финальный статус
            if file_exists:
                status = "ready"
            else:
                # Только статусы "new" и "sent" из outbox
                if outbox_status in ["new", "sent"]:
                    status = outbox_status
                else:
                    status = "sent"  # По умолчанию, если статус не "new" и не "sent"
            
            description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")
            
            result.append({
                "video_id": video_id,
                "status_code": status,
                "status_description": description
            })
        
        return {"operations": result, "total": len(result)}
    except Exception as e:
        print(f"[get_all_statuses] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Status retrieval error: {str(e)}")

@app.get("/api/result_video/{event_id}")
async def get_result_video(event_id: str, bucket: str = "videos"):
    """
    Получить результат обработки видео. JSON читается из MinIO.
    
    УПРОЩЁННАЯ АРХИТЕКТУРА:
    - Читает событие из outbox
    - Проверяет наличие JSON файла в S3 (detection/{event_id}.json)
    - Если файл есть в S3 - читает и возвращает JSON
    - Если файла нет в S3 - возвращает статус из outbox ("new" или "sent")
    """
    import asyncio
    
    loop = asyncio.get_event_loop()
    
    def get_outbox_event():
        """Читает событие из outbox"""
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        result = outbox.find_one({"_id": event_id})
        client.close()
        return result
    
    outbox_result = await loop.run_in_executor(None, get_outbox_event)
    
    # Если события нет в outbox - событие не найдено
    if not outbox_result:
        raise HTTPException(status_code=404, detail="Event not found.")
    
    outbox_status = outbox_result.get("status", "unknown")
    
    # Проверяем наличие JSON файла в S3
    detection_key = f"detection/{event_id}.json"
    
    def check_and_read_from_s3():
        """Проверяет наличие файла в S3 и читает его, если есть"""
        try:
            minio_cli = get_minio_client()
            # Пытаемся получить метаданные объекта
            minio_cli.stat_object(bucket, detection_key)
            # Если файл существует, читаем его
            response = minio_cli.get_object(bucket, detection_key)
            json_data = response.read().decode("utf-8")
            response.close()
            response.release_conn()
            return json_data, True
        except Exception as e:
            # Файл не найден или ошибка чтения
            return None, False
    
    json_data, file_exists = await loop.run_in_executor(None, check_and_read_from_s3)
    
    # Если файл есть в S3, читаем и возвращаем JSON
    if file_exists and json_data:
        try:
            import json
            prediction = json.loads(json_data)
            
            return {
                "status": "ready",
                "event_id": event_id,
                "prediction": prediction
            }
        except Exception as e:
            print(f"[get_result_video] Error parsing JSON from S3: {e}", flush=True)
            raise HTTPException(status_code=500, detail=f"JSON parsing error: {str(e)}")
    
    # Если файла нет в S3, возвращаем статус из outbox
    # Только статусы "new" и "sent" из outbox
    if outbox_status in ["new", "sent"]:
        status = outbox_status
    else:
        status = "sent"  # По умолчанию
    
    description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")
    return {
        "status": status,
        "event_id": event_id,
        "message": description,
        "ready": False
    }

@app.delete("/api/cleanup/all")
async def cleanup_all(bucket: str = "videos"):
    """
    Асинхронная ручка для полной очистки MongoDB (outbox и runner_db) и MinIO.
    Очищает все данные из всех систем в соответствии с новой микросервисной архитектурой.
    """
    import asyncio
    loop = asyncio.get_event_loop()
    
    results = {
        "mongodb": {
            "outbox_deleted": 0,
            "runner_predictions_deleted": 0
        },
        "minio": {
            "objects_deleted": 0,
            "errors": []
        }
    }
    
    try:
        # Очистка MongoDB коллекций асинхронно
        def cleanup_mongodb():
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']
            
            # Очищаем outbox
            outbox = db['outbox']
            outbox_count = outbox.count_documents({})
            outbox.delete_many({})
            
            client.close()
            
            # Очищаем runner_db (отдельная БД для сервиса runner)
            runner_client = MongoClient("mongodb://admin:password@mongo:27017/")
            runner_db = runner_client['runner_db']
            runner_predictions = runner_db['runner_predictions']
            runner_count = runner_predictions.count_documents({})
            runner_predictions.delete_many({})
            runner_client.close()
            
            return outbox_count, runner_count
        
        outbox_count, runner_count = await loop.run_in_executor(None, cleanup_mongodb)
        results["mongodb"]["outbox_deleted"] = outbox_count
        results["mongodb"]["runner_predictions_deleted"] = runner_count
        print(f"[cleanup] MongoDB: deleted {outbox_count} from outbox, {runner_count} from runner_db", flush=True)
        
        # Очистка MinIO bucket асинхронно
        def cleanup_minio():
            minio_cli = get_minio_client()
            
            # Проверяем существование bucket
            if not minio_cli.bucket_exists(bucket):
                return 0, []
            
            # Получаем все объекты в bucket (включая все префиксы: input/, result/, detection/)
            objects_to_delete = []
            try:
                objects = minio_cli.list_objects(bucket, recursive=True)
                for obj in objects:
                    objects_to_delete.append(obj.object_name)
            except Exception as e:
                return 0, [f"Error listing objects: {str(e)}"]
            
            # Удаляем объекты пакетами (MinIO поддерживает удаление до 1000 объектов за раз)
            deleted_count = 0
            errors = []
            
            # Удаляем объекты пакетами по 1000
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                try:
                    # Создаём список объектов для удаления в формате DeleteObject
                    from minio.deleteobjects import DeleteObject
                    delete_list = [DeleteObject(obj_name) for obj_name in batch]
                    
                    # Удаляем пакет
                    errors_batch = minio_cli.remove_objects(bucket, delete_list)
                    # Обрабатываем ошибки (если есть)
                    for error in errors_batch:
                        errors.append(f"Error deleting {error.object_name}: {error.error}")
                    
                    deleted_count += len(batch) - len(list(errors_batch))
                except Exception as e:
                    errors.append(f"Error deleting batch {i}-{i+len(batch)}: {str(e)}")
            
            return deleted_count, errors
        
        deleted_count, minio_errors = await loop.run_in_executor(None, cleanup_minio)
        results["minio"]["objects_deleted"] = deleted_count
        results["minio"]["errors"] = minio_errors
        
        if minio_errors:
            print(f"[cleanup] MinIO: deleted {deleted_count} objects, {len(minio_errors)} errors", flush=True)
        else:
            print(f"[cleanup] MinIO: deleted {deleted_count} objects successfully", flush=True)
        
        return {
            "status": "ok",
            "message": "Cleanup completed",
            "results": results
        }
        
    except Exception as e:
        print(f"[cleanup] Error during cleanup: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup error: {str(e)}"
        )
