"""
API Checkout Service — входная точка для загрузки видео и получения результатов

Сервис отвечает за:
1. Загрузку видео через эндпоинт /upload_video
2. Сохранение видео в объектное хранилище MinIO
3. Создание событий в outbox для асинхронной обработки (статусы: "new", "sent")
4. Получение результатов детекции и статусов для всех видео
"""
import asyncio
import io
import json
import os
import traceback
import uuid
from datetime import datetime

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from minio import Minio
from pymongo import MongoClient

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


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False


minio_client = None


def get_minio_client():
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
            print("[MinIO] Client initialized successfully", flush=True)
        except Exception as e:
            print(f"[MinIO] Error initializing client: {e}", flush=True)
            raise
    return minio_client


@app.post("/api/upload_video")
async def upload_and_enqueue_video(
    file: UploadFile = File(...),
    bucket: str = "videos"
):
    loop = asyncio.get_event_loop()

    try:
        video_id = str(uuid.uuid4())
        filename = f"input/{video_id}.mp4"
        data = await file.read()

        print(f"[upload_video] Received file: {file.filename}, size: {len(data)} bytes", flush=True)

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
            minio_task = loop.run_in_executor(None, minio_upload)
            mongo_task = loop.run_in_executor(None, mongo_insert)

            await asyncio.gather(minio_task, mongo_task)

            print(
                f"[upload_video] Successfully uploaded to MinIO and created "
                f"outbox event: {video_id}",
                flush=True
            )
        except Exception as error:
            error_msg = str(error)
            if ("MinIO" in error_msg or "minio" in error_msg.lower()
                    or "bucket" in error_msg.lower()):
                print(
                    f"[upload_video] Error uploading to MinIO: {error}",
                    flush=True
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"MinIO upload error: {error_msg}"
                )
            else:
                print(
                    f"[upload_video] Error creating outbox event: {error}",
                    flush=True
                )
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


STATUS_DESCRIPTIONS = {
    "new": "Новое событие создано, ожидает отправки в очередь обработки",
    "sent": "Событие отправлено в очередь обработки",
    "ready": "Результат сохранён в S3 и готов к забору"
}


@app.get("/api/status/all")
async def get_all_statuses():

    try:
        loop = asyncio.get_event_loop()

        def get_outbox_events():
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']
            outbox = db['outbox']
            events = list(outbox.find({}, {"_id": 1, "status": 1}))
            client.close()
            return events

        outbox_events = await loop.run_in_executor(None, get_outbox_events)


        def check_s3_file_exists(bucket, object_name):
            try:
                minio_cli = get_minio_client()
                minio_cli.stat_object(bucket, object_name)
                return True
            except Exception:
                return False

        result = []

        for event in outbox_events:
            video_id = str(event.get("_id"))
            outbox_status = event.get("status", "unknown")

            detection_key = f"detection/{video_id}.json"
            bucket = "videos" 

            file_exists = await loop.run_in_executor(
                None,
                check_s3_file_exists,
                bucket,
                detection_key
            )

            if file_exists:
                status = "ready"
            else:
                if outbox_status in ["new", "sent"]:
                    status = outbox_status
                else:
                    status = "sent" 

            description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")

            result.append({
                "video_id": video_id,
                "status_code": status,
                "status_description": description
            })

        return {"operations": result, "total": len(result)}
    except Exception as e:
        print(f"[get_all_statuses] Error: {e}", flush=True)
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Status retrieval error: {str(e)}"
        )


@app.get("/api/result_video/{event_id}")
async def get_result_video(event_id: str, bucket: str = "videos"):
    loop = asyncio.get_event_loop()

    def get_outbox_event():
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        result = outbox.find_one({"_id": event_id})
        client.close()
        return result

    outbox_result = await loop.run_in_executor(None, get_outbox_event)

    if not outbox_result:
        raise HTTPException(status_code=404, detail="Event not found.")

    outbox_status = outbox_result.get("status", "unknown")

    detection_key = f"detection/{event_id}.json"

    def check_and_read_from_s3():
        try:
            minio_cli = get_minio_client()
            minio_cli.stat_object(bucket, detection_key)
            response = minio_cli.get_object(bucket, detection_key)
            json_data = response.read().decode("utf-8")
            response.close()
            response.release_conn()
            return json_data, True
        except Exception:
            return None, False

    json_data, file_exists = await loop.run_in_executor(None, check_and_read_from_s3)

    if file_exists and json_data:
        try:
            prediction = json.loads(json_data)

            return {
                "status": "ready",
                "event_id": event_id,
                "prediction": prediction
            }
        except Exception as e:
            print(
                f"[get_result_video] Error parsing JSON from S3: {e}",
                flush=True
            )
            raise HTTPException(
                status_code=500, detail=f"JSON parsing error: {str(e)}"
            )

    if outbox_status in ["new", "sent"]:
        status = outbox_status
    else:
        status = "sent"  

    description = STATUS_DESCRIPTIONS.get(status, f"Неизвестный статус: {status}")
    return {
        "status": status,
        "event_id": event_id,
        "message": description,
        "ready": False
    }


@app.delete("/api/cleanup/all")
async def cleanup_all(bucket: str = "videos"):
    loop = asyncio.get_event_loop()

    results = {
        "mongodb": {
            "outbox_deleted": 0,
            "runner_predictions_deleted": 0
        },
        "minio": {
            "objects_deleted": 0,
            "buckets_cleaned": ["videos", "frames"],
            "errors": []
        }
    }

    try:
        def cleanup_mongodb():
            client = MongoClient("mongodb://admin:password@mongo:27017/")
            db = client['main_db']

            outbox = db['outbox']
            outbox_count = outbox.count_documents({})
            outbox.delete_many({})

            client.close()

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
        print(
            f"[cleanup] MongoDB: deleted {outbox_count} from outbox, "
            f"{runner_count} from runner_db",
            flush=True
        )

        def cleanup_minio():
            minio_cli = get_minio_client()

            total_deleted = 0
            all_errors = []

            buckets_to_clean = [bucket, "frames"]

            for bucket_name in buckets_to_clean:
                if not minio_cli.bucket_exists(bucket_name):
                    continue

                objects_to_delete = []
                try:
                    objects = minio_cli.list_objects(bucket_name, recursive=True)
                    for obj in objects:
                        objects_to_delete.append(obj.object_name)
                except Exception as e:
                    all_errors.append(f"Error listing objects in {bucket_name}: {str(e)}")
                    continue

                deleted_count = 0

                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    try:
                        from minio.deleteobjects import DeleteObject
                        delete_list = [
                            DeleteObject(obj_name) for obj_name in batch
                        ]

                        errors_batch = minio_cli.remove_objects(
                            bucket_name, delete_list
                        )
                        for error in errors_batch:
                            all_errors.append(
                                f"Error deleting {error.object_name} from "
                                f"{bucket_name}: {error.error}"
                            )

                        deleted_count += len(batch) - len(list(errors_batch))
                    except Exception as e:
                        all_errors.append(
                            f"Error deleting batch {i}-{i + len(batch)} from "
                            f"{bucket_name}: {str(e)}"
                        )

                total_deleted += deleted_count
                print(f"[cleanup] Deleted {deleted_count} objects from {bucket_name}", flush=True)

            return total_deleted, all_errors

        deleted_count, minio_errors = await loop.run_in_executor(None, cleanup_minio)
        results["minio"]["objects_deleted"] = deleted_count
        results["minio"]["errors"] = minio_errors

        if minio_errors:
            print(
                f"[cleanup] MinIO: deleted {deleted_count} objects, "
                f"{len(minio_errors)} errors",
                flush=True
            )
        else:
            print(
                f"[cleanup] MinIO: deleted {deleted_count} objects successfully",
                flush=True
            )

        return {
            "status": "ok",
            "message": "Cleanup completed",
            "results": results
        }

    except Exception as e:
        print(f"[cleanup] Error during cleanup: {e}", flush=True)
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cleanup error: {str(e)}"
        )