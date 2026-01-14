"""
API Checkout Service — входная точка для загрузки видео и получения результатов

Сервис отвечает за:
1. Загрузку видео через эндпоинт /upload_video
2. Сохранение видео в объектное хранилище MinIO
3. Создание событий в outbox для асинхронной обработки
4. Получение результатов детекции
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

        try:
            await loop.run_in_executor(None, minio_upload)
            print(f"[upload_video] Successfully uploaded to MinIO", flush=True)
        except Exception as upload_error:
            print(f"[upload_video] Error uploading to MinIO: {upload_error}", flush=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO upload error: {str(upload_error)}"
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

        try:
            await loop.run_in_executor(None, mongo_insert)
            print(f"[upload_video] Created outbox event: {video_id}", flush=True)
        except Exception as mongo_error:
            print(f"[upload_video] Error creating outbox event: {mongo_error}", flush=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MongoDB event creation error: {str(mongo_error)}"
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
# ОБНОВЛЁННЫЕ СТАТУСЫ (новая архитектура обработки по кадрам):
STATUS_DESCRIPTIONS = {
    "new": "Новое событие создано, ожидает отправки в очередь обработки",
    "sent": "Событие отправлено в очередь обработки",
    "in progress": "Видео обрабатывается в сервисе предобработки (runner) или inference (обработка кадров)",
    "have ready result": "Детекция завершена, все кадры обработаны, результат готовится к сохранению",
    "predicted": "Результат готов и доступен для забора"
}

@app.get("/api/status/all")
async def get_all_statuses():
    """
    Получить статусы всех операций (ID Видео - код статуса - расшифровка - время обработки)
    
    Возвращает:
    - video_id: идентификатор видео
    - status_code: код статуса (new, sent, in progress, have ready result, predicted)
    - status_description: расшифровка статуса на русском языке
    - processing_time_seconds: время, прошедшее с начала обработки (в секундах)
    - processing_time_formatted: время обработки в читаемом формате (например, "2m 30s")
    """
    import asyncio
    from datetime import datetime, timedelta
    
    try:
        # Читаем события из MongoDB асинхронно
        loop = asyncio.get_event_loop()
        
        def get_events():
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']
            outbox = db['outbox']
            events = list(outbox.find({}, {"_id": 1, "status": 1, "created_at": 1, "predicted_at": 1}))
            client.close()
            return events
        
        events = await loop.run_in_executor(None, get_events)
        
        result = []
        now = datetime.utcnow()
        
        for event in events:
            video_id = event.get("_id")
            status = event.get("status", "unknown")
            description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")
            
            # Вычисляем время обработки
            # Если статус "predicted", используем predicted_at, иначе текущее время
            created_at = event.get("created_at")
            predicted_at = event.get("predicted_at")
            processing_time_seconds = None
            processing_time_formatted = None
            
            if created_at:
                # Определяем конечное время: если статус "predicted" и есть predicted_at, используем его, иначе текущее время
                end_time = now
                if status == "predicted" and predicted_at:
                    if isinstance(predicted_at, datetime):
                        end_time = predicted_at
                    elif isinstance(predicted_at, str):
                        try:
                            predicted_dt = datetime.fromisoformat(predicted_at.replace('Z', '+00:00'))
                            end_time = predicted_dt.replace(tzinfo=None)
                        except:
                            end_time = now
                
                # Если created_at - это datetime объект
                if isinstance(created_at, datetime):
                    time_diff = end_time - created_at
                # Если created_at - это строка, пытаемся распарсить
                elif isinstance(created_at, str):
                    try:
                        created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        time_diff = end_time - created_dt.replace(tzinfo=None)
                    except:
                        time_diff = None
                else:
                    time_diff = None
                
                if time_diff:
                    processing_time_seconds = int(time_diff.total_seconds())
                    # Форматируем время в читаемый вид
                    hours = processing_time_seconds // 3600
                    minutes = (processing_time_seconds % 3600) // 60
                    seconds = processing_time_seconds % 60
                    
                    if hours > 0:
                        processing_time_formatted = f"{hours}h {minutes}m {seconds}s"
                    elif minutes > 0:
                        processing_time_formatted = f"{minutes}m {seconds}s"
                    else:
                        processing_time_formatted = f"{seconds}s"
            
            result.append({
                "video_id": video_id,
                "status_code": status,
                "status_description": description,
                "processing_time_seconds": processing_time_seconds,
                "processing_time_formatted": processing_time_formatted
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
    Получить результат обработки видео. JSON читается из MinIO, а не из MongoDB.
    
    Также возвращает время обработки видео.
    """
    import asyncio
    from datetime import datetime
    
    # Читаем статус из MongoDB асинхронно
    loop = asyncio.get_event_loop()
    
    def get_status():
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        result = outbox.find_one({"_id": event_id})
        client.close()
        return result
    
    result = await loop.run_in_executor(None, get_status)
    
    if not result:
        raise HTTPException(status_code=404, detail="Event not found.")
    
    status = result.get("status", "unknown")
    
    # Вычисляем время обработки
    created_at = result.get("created_at")
    predicted_at = result.get("predicted_at")
    processing_time_seconds = None
    processing_time_formatted = None
    
    if created_at:
        # Определяем конечное время: если статус "predicted" и есть predicted_at, используем его, иначе текущее время
        now = datetime.utcnow()
        end_time = now
        if status == "predicted" and predicted_at:
            if isinstance(predicted_at, datetime):
                end_time = predicted_at
            elif isinstance(predicted_at, str):
                try:
                    predicted_dt = datetime.fromisoformat(predicted_at.replace('Z', '+00:00'))
                    end_time = predicted_dt.replace(tzinfo=None)
                except:
                    end_time = now
        
        # Если created_at - это datetime объект
        if isinstance(created_at, datetime):
            time_diff = end_time - created_at
        # Если created_at - это строка, пытаемся распарсить
        elif isinstance(created_at, str):
            try:
                created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                time_diff = end_time - created_dt.replace(tzinfo=None)
            except:
                time_diff = None
        else:
            time_diff = None
        
        if time_diff:
            processing_time_seconds = int(time_diff.total_seconds())
            hours = processing_time_seconds // 3600
            minutes = (processing_time_seconds % 3600) // 60
            seconds = processing_time_seconds % 60
            
            if hours > 0:
                processing_time_formatted = f"{hours}h {minutes}m {seconds}s"
            elif minutes > 0:
                processing_time_formatted = f"{minutes}m {seconds}s"
            else:
                processing_time_formatted = f"{seconds}s"
    
    # Если результат готов (статус "predicted"), читаем JSON из MinIO
    if status == "predicted":
        try:
            output_s3 = result.get("output_s3", {})
            detection_key = output_s3.get("object")
            detection_bucket = output_s3.get("bucket", bucket)
            
            if not detection_key:
                raise HTTPException(status_code=404, detail="Result path not found in database.")
            
            # Читаем JSON из MinIO асинхронно
            def read_from_minio():
                minio_cli = get_minio_client()
                response = minio_cli.get_object(detection_bucket, detection_key)
                json_data = response.read().decode("utf-8")
                response.close()
                response.release_conn()
                return json_data
            
            json_data = await loop.run_in_executor(None, read_from_minio)
            
            import json
            prediction = json.loads(json_data)
            
            return {
                "status": status,
                "event_id": event_id,
                "prediction": prediction,
                "processing_time_seconds": processing_time_seconds,
                "processing_time_formatted": processing_time_formatted
            }
        except Exception as e:
            print(f"[get_result_video] Error reading from MinIO: {e}", flush=True)
            raise HTTPException(status_code=500, detail=f"MinIO result read error: {str(e)}")
    
    # Если результат не готов, возвращаем текущий статус с пояснением
    description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")
    return {
        "status": status,
        "event_id": event_id,
        "message": description,
        "ready": False,
        "processing_time_seconds": processing_time_seconds,
        "processing_time_formatted": processing_time_formatted
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
