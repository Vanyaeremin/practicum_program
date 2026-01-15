"""
Runner Service — сервис предобработки видео

Архитектура (микросервисная с отдельной БД):
1. Читает события из Kafka темы "outbox_events"
2. Скачивает видео из MinIO (input/{video_id}.mp4)
3. Делит видео на кадры (извлекает каждый 5-й кадр)
4. Предобрабатывает каждый кадр отдельно (Gaussian blur + корректировка контраста)
5. Сохраняет каждый предобработанный кадр в S3 бакет "frames" по пути
   frames/{event_id}/{frame_id}.jpg
6. Отправляет задачу обработки кадра в inference через Kafka тему "inference_tasks"
   (с путём к кадру в S3)
7. Читает результаты инференса по кадрам из Kafka темы "inference_results"
8. Сохраняет JSON по кадрам в отдельную БД runner_db.runner_predictions
9. Когда все кадры обработаны, объединяет JSON и устанавливает статус "predictable" в runner_db
10. Outbox S3 worker забирает данные из runner_db при статусе "predictable"
    и сохраняет в S3, затем меняет статус на "ready"

Важные особенности:
- Предобработанное видео НЕ сохраняется в MinIO
- Каждый кадр обрабатывается и сохраняется в S3 отдельно
- Кадры передаются в inference через S3, а не через Kafka (более корректно для больших файлов)
- В Kafka передаётся только путь к кадру в S3, а не сам кадр
- Очерёдность кадров обеспечивается через consumer groups в Kafka и key-based partitioning
- Все операции полностью асинхронные
- Используется отдельная БД runner_db для поддержания микросервисной архитектуры
- Runner НЕ обновляет outbox (только runner_db) - это соответствует принципам микросервисов
- Статусы в runner_db: "processing" -> "predictable" -> "ready"
- Данные остаются в runner_db после сохранения в S3 (для истории и отладки)

Шаги предобработки кадра (см. frame_extractor.py):
- Gaussian blur (ksize=3)
- Контраст/яркость (alpha=1.3, beta=5)
"""
import asyncio
import json
import os
import traceback
from concurrent.futures import ProcessPoolExecutor
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from pymongo import MongoClient
from frame_extractor import extract_and_preprocess_frames_async
from kafka_producer import RunnerKafkaProducer
from s3_utils import (
    check_s3_availability,
    download_image,
    upload_frame
)

app = FastAPI()

executor = ProcessPoolExecutor(max_workers=os.cpu_count())

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")
RUNNER_MONGO_URI = os.getenv("RUNNER_MONGO_URI", "mongodb://admin:password@mongo:27017/")


def get_runner_db():
    client = MongoClient(RUNNER_MONGO_URI)
    return client['runner_db']


# Глобальный словарь для отслеживания активных задач обработки видео
active_video_tasks = {}


async def send_frames_ordered(event_id, frames, bucket, producer):
    loop = asyncio.get_running_loop()

    frames_bucket = "frames"

    upload_tasks = []
    for frame_id, frame_bytes in frames:
        object_name = f"frames/{event_id}/{frame_id}.jpg"
        upload_tasks.append(
            (frame_id, loop.run_in_executor(
                None, upload_frame, frames_bucket, object_name, frame_bytes
            ))
        )

    uploaded_results = []
    for frame_id, upload_task in upload_tasks:
        await upload_task
        object_name = f"frames/{event_id}/{frame_id}.jpg"
        uploaded_results.append((frame_id, object_name))
        print(
            f"[runner] Uploaded frame {frame_id} to S3: {frames_bucket}/{object_name}",
            flush=True
        )

    frames_sent = 0
    try:
        for frame_id, frame_path in uploaded_results:
            await producer.send({
                "event_id": event_id,
                "frame_id": frame_id,  
                "bucket": frames_bucket,  
                "frame_path": frame_path, 
                "media_type": "frame"
            })
            frames_sent += 1
            print(f"[runner] Sent frame {frame_id} task to Kafka for {event_id}", flush=True)

        print(f"[runner] All {len(frames)} frames sent in order for {event_id}", flush=True)
        return frames_sent
    except Exception as e:
        print(f"[runner] Error sending frames for {event_id}: {e}", flush=True)
        raise


async def process_video_task(event_id, payload, consumer, msg):
    try:
        bucket = payload.get('bucket')
        object_name = payload.get('object')
        media_type = payload.get('media_type', 'image')

        loop = asyncio.get_running_loop()
        s3_available = False
        while not s3_available:
            s3_available = await loop.run_in_executor(
                None, check_s3_availability, "runner"
            )
            if not s3_available:
                print(
                    "[runner] S3/MinIO not ready yet, retry in 5s...",
                    flush=True
                )
                await asyncio.sleep(5)

        print(f"[runner] Processing video {event_id} in parallel", flush=True)

        print(
            f"[runner] Downloading file: bucket='{bucket}', "
            f"object_name='{object_name}' for event_id={event_id}",
            flush=True
        )
        
        try:
            file_bytes = await loop.run_in_executor(
                None, download_image, bucket, object_name
            )
            print(
                f"[runner] File downloaded for {event_id} ({len(file_bytes)} bytes)",
                flush=True
            )
        except Exception as download_error:
            error_str = str(download_error)
            if "NoSuchKey" in error_str or "does not exist" in error_str.lower() or "not found" in error_str.lower():
                print(
                    f"[runner] File not found in S3: {bucket}/{object_name} "
                    f"for {event_id}: {download_error}",
                    flush=True
                )

                def set_error_status():
                    db = get_runner_db()
                    runner_predictions = db['runner_predictions']
                    runner_predictions.update_one(
                        {"_id": event_id},
                        {
                            "$set": {
                                "event_id": event_id,
                                "bucket": bucket,
                                "object_name": object_name,
                                "status": "error",
                                "error_message": (
                                    f"File not found in S3: {bucket}/{object_name}"
                                ),
                                "error_type": "NoSuchKey"
                            }
                        },
                        upsert=True
                    )

                await loop.run_in_executor(None, set_error_status)
                print(
                    f"[runner] Set error status in runner_db for {event_id}",
                    flush=True
                )

                raise FileNotFoundError(
                    f"File not found in S3: {bucket}/{object_name}"
                )
            raise

        frames = await extract_and_preprocess_frames_async(
            file_bytes,
            every_n_frames=5,
            blur_ksize=3,
            alpha=1.3,
            beta=5
        )
        print(
            f"[runner] Extracted {len(frames)} frames from video {event_id}",
            flush=True
        )

        def init_frame_tracking():
            db = get_runner_db()
            runner_predictions = db['runner_predictions']
            runner_predictions.update_one(
                {"_id": event_id},
                {
                    "$set": {
                        "event_id": event_id,
                        "bucket": bucket,
                        "total_frames": len(frames),
                        "received_frames": 0,
                        "frame_results": {},
                        "status": "processing"
                    }
                },
                upsert=True
            )

        await loop.run_in_executor(None, init_frame_tracking)

        producer = RunnerKafkaProducer()
        await producer.start()

        try:
            frames_sent = await send_frames_ordered(
                event_id, frames, bucket, producer
            )
            print(
                f"[runner] Sent all {frames_sent} frames for {event_id}",
                flush=True
            )
        except Exception as send_error:
            print(
                f"[runner] Error sending frames for {event_id}: {send_error}",
                flush=True
            )
            raise
        finally:
            await producer.stop()

    except FileNotFoundError as e:
        print(f"[runner] File not found for {event_id}: {e}", flush=True)
        raise
    except Exception as e:
        error_str = str(e)
        if "NoSuchKey" in error_str or "does not exist" in error_str.lower():
            print(
                f"[runner] File not found (NoSuchKey) for {event_id}: {e}",
                flush=True
            )

            def set_error_status():
                db = get_runner_db()
                runner_predictions = db['runner_predictions']
                runner_predictions.update_one(
                    {"_id": event_id},
                    {
                        "$set": {
                            "event_id": event_id,
                            "bucket": payload.get('bucket'),
                            "object_name": payload.get('object'),
                            "status": "error",
                            "error_message": "File not found in S3",
                            "error_type": "NoSuchKey"
                        }
                    },
                    upsert=True
                )

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, set_error_status)
            print(
                f"[runner] Set error status in runner_db for {event_id}",
                flush=True
            )

            raise FileNotFoundError(f"File not found in S3: {object_name}")

        print(f"[runner] Error processing video {event_id}: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        if event_id in active_video_tasks:
            del active_video_tasks[event_id]


async def consume():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "outbox_events",
                bootstrap_servers="kafka:9092",
                group_id="runner-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[runner] aiokafka consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[runner] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)

    max_concurrent_videos = 5
    semaphore = asyncio.Semaphore(max_concurrent_videos)

    print("[runner] Starting parallel video processing...", flush=True)

    try:
        async for msg in consumer:
            try:
                event_id = msg.value.get("_id")
                if not event_id:
                    payload = msg.value.get("payload", {})
                    object_name = payload.get('object')
                    if object_name:
                        event_id = (
                            object_name.split('/')[-1]
                            .replace('.mp4', '')
                            .replace('.jpg', '')
                            .replace('.jpeg', '')
                        )
                    else:
                        print(
                            f"[runner] No event_id or object_name in message: "
                            f"{msg.value}",
                            flush=True
                        )
                        await consumer.commit()
                        continue

                payload = msg.value.get("payload", {})
                bucket = payload.get('bucket')
                object_name = payload.get('object')

                if not bucket or not object_name:
                    print(
                        f"[runner] No S3 info in message for {event_id}: {msg.value}",
                        flush=True
                    )
                    await consumer.commit()
                    continue

                print(
                    f"[runner] Received message for event_id={event_id}, "
                    f"bucket={bucket}, object={object_name}",
                    flush=True
                )

                if event_id in active_video_tasks:
                    print(
                        f"[runner] Video {event_id} already being processed, "
                        f"skipping and committing",
                        flush=True
                    )
                    await consumer.commit()
                    continue

                async def process_with_semaphore(
                    event_id_local, payload_local, consumer_local, msg_local
                ):
                    async with semaphore:
                        task = None
                        try:
                            task = asyncio.create_task(
                                process_video_task(
                                    event_id_local,
                                    payload_local,
                                    consumer_local,
                                    msg_local
                                )
                            )
                            active_video_tasks[event_id_local] = task
                            await task
                            await consumer_local.commit()
                            print(
                                f"[runner] Committed message for {event_id_local} "
                                f"after successful processing",
                                flush=True
                            )
                        except FileNotFoundError as file_error:
                            print(
                                f"[runner] File not found for {event_id_local}, "
                                f"committing message to skip: {file_error}",
                                flush=True
                            )
                            await consumer_local.commit()
                            print(
                                f"[runner] Committed message for {event_id_local} "
                                f"after file not found error",
                                flush=True
                            )
                        except Exception as task_error:
                            print(
                                f"[runner] Error in background task for "
                                f"{event_id_local}: {task_error}",
                                flush=True
                            )
                            traceback.print_exc()
                        finally:
                            if event_id_local in active_video_tasks:
                                del active_video_tasks[event_id_local]
                            if task and not task.done():
                                task.cancel()
                                try:
                                    await task
                                except asyncio.CancelledError:
                                    pass
                            print(
                                f"[runner] Released semaphore slot for "
                                f"{event_id_local}",
                                flush=True
                            )

                asyncio.create_task(
                    process_with_semaphore(event_id, payload, consumer, msg)
                )
                print(
                    f"[runner] Started parallel processing for {event_id} "
                    f"(active tasks: {len(active_video_tasks)})",
                    flush=True
                )
            except Exception as e:
                print(f"[runner] Error in message handling: {e}", flush=True)
                traceback.print_exc()
    finally:
        await consumer.stop()
        if event_id in active_video_tasks:
            del active_video_tasks[event_id]


async def consume_inference_results():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "inference_results",
                bootstrap_servers="kafka:9092",
                group_id="runner-results-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[runner] inference_results consumer connected", flush=True)
            break
        except Exception as e:
            print(
                f"[runner] inference_results consumer not ready yet, retry in 5s... {e}",
                flush=True
            )
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            try:
                event_id = msg.value.get("event_id")
                frame_id = msg.value.get("frame_id") 
                bucket = msg.value.get("bucket")
                frame_prediction = msg.value.get(
                    "prediction"
                )  

                if (
                    not event_id or frame_id is None or not bucket or not frame_prediction
                ):
                    print(f"[runner] Invalid message format: {msg.value}", flush=True)
                    continue

                print(
                    f"[runner] Received inference result for event_id={event_id}, "
                    f"frame_id={frame_id}",
                    flush=True
                )

                loop = asyncio.get_running_loop()

                def save_frame_and_check_complete():
                    db = get_runner_db()
                    runner_predictions = db['runner_predictions']
                    doc = runner_predictions.find_one({"_id": event_id})

                    if not doc:
                        print(
                            f"[runner] No tracking document found in runner_db "
                            f"for event_id={event_id}, skipping",
                            flush=True
                        )
                        return

                    frame_results = doc.get("frame_results", {})
                    frame_id_str = str(frame_id)

                    if frame_id_str in frame_results:
                        print(
                            f"[runner] Frame {frame_id} for event_id={event_id} "
                            f"already processed (idempotency check), skipping",
                            flush=True
                        )
                        return

                    result = runner_predictions.update_one(
                        {
                            "_id": event_id,
                            f"frame_results.{frame_id_str}": {"$exists": False}
                        },
                        {
                            "$set": {
                                f"frame_results.{frame_id_str}": frame_prediction
                            },
                            "$inc": {
                                "received_frames": 1
                            }
                        }
                    )

                    if result.modified_count == 0:
                        print(
                            f"[runner] Frame {frame_id} for event_id={event_id} "
                            f"was already processed by another worker "
                            f"(idempotency check)",
                            flush=True
                        )
                        return

                    updated_doc = runner_predictions.find_one({"_id": event_id})
                    received_frames = updated_doc.get("received_frames", 0)
                    total_frames = updated_doc.get("total_frames", 0)

                    print(
                        f"[runner] Saved frame {frame_id} result for event_id={event_id} "
                        f"({received_frames}/{total_frames} frames received)",
                        flush=True
                    )

                    if received_frames >= total_frames:
                        print(
                            f"[runner] All frames received for event_id={event_id}, "
                            f"merging results...",
                            flush=True
                        )

                        final_doc = runner_predictions.find_one({"_id": event_id})
                        if final_doc.get("status") == "predictable":
                            print(
                                f"[runner] Status already set to 'predictable' "
                                f"for event_id={event_id} (idempotency check)",
                                flush=True
                            )
                            return

                        final_frame_results = final_doc.get("frame_results", {})
                        sorted_frame_ids = sorted(
                            [int(fid) for fid in final_frame_results.keys()]
                        )

                        final_prediction = []
                        for fid in sorted_frame_ids:
                            frame_result = final_frame_results[str(fid)]
                            final_prediction.append(frame_result)

                        update_result = runner_predictions.update_one(
                            {
                                "_id": event_id,
                                "status": {"$ne": "predictable"}
                            },
                            {
                                "$set": {
                                    "prediction": final_prediction,
                                    "status": "predictable"
                                }
                            }
                        )

                        if update_result.modified_count == 0:
                            print(
                                f"[runner] Status was already set to 'predictable' "
                                f"for event_id={event_id} (idempotency check)",
                                flush=True
                            )
                        else:
                            print(
                                f"[runner] Merged {len(final_prediction)} frame results "
                                f"for event_id={event_id}, status set to 'predictable' "
                                f"in runner_db",
                                flush=True
                            )

                await loop.run_in_executor(None, save_frame_and_check_complete)

                await consumer.commit()
                print(
                    f"[runner] Committed inference result message for "
                    f"event_id={event_id}, frame_id={frame_id}",
                    flush=True
                )

            except Exception as e:
                print(
                    f"[runner] Error processing inference result: {e}",
                    flush=True
                )
                traceback.print_exc()
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume())  # Consumer для предобработки видео
    asyncio.create_task(consume_inference_results())  # Consumer для сохранения результатов в S3


@app.get("/health")
def health():
    return {"status": "ok"}