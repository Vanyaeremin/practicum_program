"""
Runner Service — сервис предобработки видео

АРХИТЕКТУРА (микросервисная с отдельной БД):
1. Читает события из Kafka темы "outbox_events"
2. Скачивает видео из MinIO (input/{video_id}.mp4)
3. Делит видео на кадры (извлекает каждый 5-й кадр)
4. Предобрабатывает каждый кадр отдельно (Gaussian blur + корректировка контраста)
5. Отправляет каждый предобработанный кадр в inference через Kafka тему "inference_tasks"
6. Читает результаты инференса по кадрам из Kafka темы "inference_results"
7. Сохраняет JSON по кадрам в отдельную БД runner_db.runner_predictions
8. Когда все кадры обработаны, объединяет JSON и устанавливает статус "predictable" в runner_db
9. Outbox S3 worker забирает данные из runner_db при статусе "predictable" и сохраняет в S3, затем меняет статус на "ready"

ВАЖНО:
- Предобработанное видео НЕ сохраняется в MinIO
- Каждый кадр обрабатывается и отправляется в inference отдельно
- Очерёдность кадров обеспечивается через consumer groups в Kafka
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
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from pymongo import MongoClient
import os

app = FastAPI()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")
RUNNER_MONGO_URI = os.getenv("RUNNER_MONGO_URI", "mongodb://admin:password@mongo:27017/")

def get_runner_db():
    """Получает подключение к runner_db"""
    client = MongoClient(RUNNER_MONGO_URI)
    return client['runner_db']

def check_s3_availability():
    """Проверяет доступность S3/MinIO"""
    try:
        from s3_utils import client
        # Пытаемся получить список бакетов для проверки соединения
        client.list_buckets()
        return True
    except Exception as e:
        print(f"[runner] S3/MinIO not available: {e}", flush=True)
        try:
            from s3_utils import recreate_client
            new_client = recreate_client()
            new_client.list_buckets()
            return True
        except Exception as reinit_error:
            print(f"[runner] Reinitialization failed: {reinit_error}", flush=True)
            return False

# Словарь для отслеживания активных задач обработки видео
active_video_tasks = {}

async def send_frames_ordered(event_id, frames, bucket, producer):
    """
    Отправляет кадры одного видео с сохранением порядка.
    
    ОПТИМИЗАЦИЯ:
    - Кодирование base64 выполняется параллельно (CPU-bound)
    - Отправка в Kafka выполняется последовательно для гарантии порядка
    - Kafka key (event_id) дополнительно гарантирует порядок в partition
    """
    import base64
    loop = asyncio.get_running_loop()
    
    # ШАГ 1: Кодируем все кадры параллельно (CPU-bound операция)
    # Это не нарушает порядок, так как мы сохраняем соответствие frame_id
    encode_tasks = [
        (frame_id, loop.run_in_executor(None, base64.b64encode, frame_bytes))
        for frame_id, frame_bytes in frames
    ]
    
    # Ждём завершения всех кодирований, сохраняя порядок
    encoded_results = []
    for frame_id, encode_task in encode_tasks:
        frame_base64_bytes = await encode_task
        frame_base64 = frame_base64_bytes.decode('utf-8')
        encoded_results.append((frame_id, frame_base64))
    
    # ШАГ 2: Отправляем кадры последовательно для гарантии порядка
    # Kafka key (event_id) дополнительно гарантирует порядок в partition
    frames_sent = 0
    try:
        for frame_id, frame_base64 in encoded_results:
            await producer.send({
                "event_id": event_id,
                "frame_id": frame_id,  # Важно для правильной сборки
                "bucket": bucket,
                "frame_data": frame_base64,
                "media_type": "frame"
            })
            frames_sent += 1
            print(f"[runner] Sent frame {frame_id} for {event_id}", flush=True)
        
        print(f"[runner] All {len(frames)} frames sent in order for {event_id}", flush=True)
        return frames_sent
    except Exception as e:
        print(f"[runner] Error sending frames for {event_id}: {e}", flush=True)
        raise

async def process_video_task(event_id, payload, consumer, msg):
    """
    Обрабатывает одно видео: скачивает, извлекает кадры и отправляет их.
    Эта функция может выполняться параллельно для разных видео.
    
    ИЗМЕНЕНИЕ: consumer.commit() убран отсюда, так как коммит происходит в consume()
    ПОСЛЕ успешной обработки этой функции. Это гарантирует, что сообщение не будет потеряно
    при сбое во время обработки - оно будет обработано повторно после перезапуска.
    """
    try:
        bucket = payload.get('bucket')
        object_name = payload.get('object')
        media_type = payload.get('media_type', 'image')
        
        # Проверка доступности S3
        s3_available = False
        while not s3_available:
            loop = asyncio.get_running_loop()
            s3_available = await loop.run_in_executor(None, check_s3_availability)
            if not s3_available:
                print(f"[runner] S3/MinIO not ready yet, retry in 5s...", flush=True)
                await asyncio.sleep(5)
        
        print(f"[runner] Processing video {event_id} in parallel", flush=True)
        
        # Скачиваем файл
        from s3_utils import download_image
        loop = asyncio.get_running_loop()
        file_bytes = await loop.run_in_executor(None, download_image, bucket, object_name)
        print(f"[runner] File downloaded for {event_id} ({len(file_bytes)} bytes)", flush=True)
        
        if media_type == 'video':
            # Извлекаем кадры
            from frame_extractor import extract_and_preprocess_frames_async
            frames = await extract_and_preprocess_frames_async(
                file_bytes,
                every_n_frames=5,
                blur_ksize=3,
                alpha=1.3,
                beta=5
            )
            print(f"[runner] Extracted {len(frames)} frames from video {event_id}", flush=True)
            
            # Инициализируем трекинг
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
            
            # Отправляем кадры с сохранением порядка
            from kafka_producer import RunnerKafkaProducer
            producer = RunnerKafkaProducer()
            await producer.start()
            
            try:
                frames_sent = await send_frames_ordered(event_id, frames, bucket, producer)
                # ✅ УБРАНО: await consumer.commit() - коммит уже произошел в consume()
                print(f"[runner] Sent all {frames_sent} frames for {event_id}", flush=True)
            except Exception as send_error:
                print(f"[runner] Error sending frames for {event_id}: {send_error}", flush=True)
                raise
            finally:
                await producer.stop()
        else:
            # Обработка изображений
            from async_wrapper import preprocess_image_async
            processed_bytes = await preprocess_image_async(file_bytes)
            print(f"[runner] Image processed! Bytes: {len(processed_bytes)}", flush=True)
            # ✅ УБРАНО: await consumer.commit() - коммит уже произошел в consume()
        
    except Exception as e:
        print(f"[runner] Error processing video {event_id}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Удаляем задачу из активных
        if event_id in active_video_tasks:
            del active_video_tasks[event_id]

async def consume():
    """
    Оптимизированный consumer с параллельной обработкой разных видео.
    
    КЛЮЧЕВЫЕ ОСОБЕННОСТИ:
    1. Разные видео обрабатываются параллельно (разные event_id)
    2. Кадры одного видео отправляются последовательно (сохранение порядка)
    3. Kafka key (event_id) гарантирует порядок в partition
    4. Используется asyncio.Semaphore для ограничения параллелизма
    
    ИСПРАВЛЕНИЯ:
    - Коммит offset'а происходит ПОСЛЕ успешной обработки сообщения
    - Это гарантирует, что сообщение не будет потеряно при сбое во время обработки
    - При ошибке коммит не происходит, сообщение будет обработано повторно
    - Правильная обработка дубликатов с коммитом
    - Обработка ошибок в фоновых задачах
    """
    # Ожидаем готовности Kafka
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
    
    # Семафор для ограничения параллелизма (например, максимум 5 видео одновременно)
    max_concurrent_videos = 5
    semaphore = asyncio.Semaphore(max_concurrent_videos)
    
    print("[runner] Starting parallel video processing...", flush=True)
    
    try:
        async for msg in consumer:
            try:
                payload = msg.value.get("payload", {})
                bucket = payload.get('bucket')
                object_name = payload.get('object')
                
                if not bucket or not object_name:
                    print(f"[runner] No S3 info in message: {msg.value}", flush=True)
                    await consumer.commit()  # Коммитим невалидные сообщения, чтобы не обрабатывать их снова
                    continue
                
                event_id = object_name.split('/')[-1].replace('.mp4', '').replace('.jpg', '').replace('.jpeg', '')
                
                # Проверяем, не обрабатывается ли уже это видео
                if event_id in active_video_tasks:
                    print(f"[runner] Video {event_id} already being processed, skipping and committing", flush=True)
                    await consumer.commit()  # Коммитим дубликаты, чтобы не зациклиться
                    continue
                
                # ❌ УБРАНО: await consumer.commit() - коммит будет после успешной обработки
                
                # Создаём задачу для обработки видео
                async def process_with_semaphore():
                    async with semaphore:  # Ограничиваем параллелизм
                        task = asyncio.create_task(
                            process_video_task(event_id, payload, consumer, msg)
                        )
                        active_video_tasks[event_id] = task
                        try:
                            await task
                            # ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Коммитим offset ТОЛЬКО после успешной обработки
                            # Это гарантирует, что сообщение не будет потеряно при сбое во время обработки
                            await consumer.commit()
                            print(f"[runner] Committed message for {event_id} after successful processing", flush=True)
                        except Exception as task_error:
                            # ✅ ИСПРАВЛЕНИЕ: Логируем ошибки в фоновых задачах
                            # ❌ НЕ коммитим при ошибке - сообщение будет обработано повторно после перезапуска
                            print(f"[runner] Error in background task for {event_id}: {task_error}", flush=True)
                            import traceback
                            traceback.print_exc()
                            # Сообщение останется в Kafka и будет обработано повторно
                        finally:
                            if event_id in active_video_tasks:
                                del active_video_tasks[event_id]
                
                # Запускаем обработку в фоне (коммит произойдёт после успешной обработки)
                asyncio.create_task(process_with_semaphore())
                print(f"[runner] Started parallel processing for {event_id}", flush=True)
                
            except Exception as e:
                print(f"[runner] Error in message handling: {e}", flush=True)
                import traceback
                traceback.print_exc()
                # ❌ НЕ коммитим при ошибке - сообщение будет обработано повторно
                # Это гарантирует, что сообщение не будет потеряно
    finally:
        await consumer.stop()

async def consume_inference_results():
    """
    Чтение результатов инференса по кадрам из Kafka и сборка финального JSON.
    
    ЛОГИКА (микросервисная архитектура):
    1. Получает JSON результат для одного кадра от inference
    2. Сохраняет результат кадра в runner_db.runner_predictions
    3. Проверяет, все ли кадры получены
    4. Когда все кадры получены, объединяет их в правильном порядке (по frame_id)
    5. Устанавливает статус "predictable" в runner_db (готов к сохранению в S3)
    6. Outbox S3 worker забирает данные из runner_db при статусе "predictable" и сохраняет в S3, затем меняет статус на "ready"
    
    ОЧЕРЁДНОСТЬ КАДРОВ:
    Обеспечивается через frame_id в сообщениях Kafka и сортировку при объединении.
    Каждый кадр имеет уникальный frame_id, который соответствует его позиции в исходном видео.
    При объединении кадры сортируются по frame_id для сохранения правильного порядка.
    
    МИКРОСЕРВИТНАЯ АРХИТЕКТУРА:
    - runner_db используется для хранения результатов обработки кадров
    - Runner НЕ обновляет outbox (только runner_db) - это соответствует принципам микросервисов
    - Статусы в runner_db: "processing" -> "predictable" -> "ready"
    - Данные остаются в runner_db после сохранения в S3
    - Разделение БД поддерживает независимость сервисов
    """
    # Ожидаем готовности Kafka
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "inference_results",  # Тема Kafka с результатами инференса по кадрам
                bootstrap_servers="kafka:9092",
                group_id="runner-results-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Отключаем авто-коммит для надежности
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[runner] inference_results consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[runner] inference_results consumer not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
    
    try:
        async for msg in consumer:
            try:
                event_id = msg.value.get("event_id")
                frame_id = msg.value.get("frame_id")  # Номер кадра в исходном видео
                bucket = msg.value.get("bucket")
                frame_prediction = msg.value.get("prediction")  # JSON результат для одного кадра
                
                if not event_id or frame_id is None or not bucket or not frame_prediction:
                    print(f"[runner] Invalid message format: {msg.value}", flush=True)
                    continue
                
                print(f"[runner] Received inference result for event_id={event_id}, frame_id={frame_id}", flush=True)
                
                # Сохраняем результат кадра в промежуточную БД и проверяем, все ли кадры получены
                loop = asyncio.get_running_loop()
                
                def save_frame_and_check_complete():
                    """
                    Сохраняет результат кадра в runner_db и проверяет, все ли кадры обработаны.
                    Если все кадры получены, объединяет их в финальный JSON.
                    
                    ВАЖНО: Runner НЕ обновляет outbox - это соответствует принципам микросервисов.
                    Статусы хранятся только в runner_db.
                    
                    ИСПРАВЛЕНИЕ (Пункт 4): Добавлена проверка идемпотентности - проверяем, был ли frame_id уже обработан.
                    """
                    # Используем отдельную БД runner_db для результатов
                    db = get_runner_db()
                    runner_predictions = db['runner_predictions']
                    
                    # Получаем текущее состояние обработки видео из runner_db
                    doc = runner_predictions.find_one({"_id": event_id})
                    
                    if not doc:
                        print(f"[runner] No tracking document found in runner_db for event_id={event_id}, skipping", flush=True)
                        return
                    
                    # ИСПРАВЛЕНИЕ (Пункт 4): Проверяем идемпотентность - был ли frame_id уже обработан
                    frame_results = doc.get("frame_results", {})
                    frame_id_str = str(frame_id)
                    
                    if frame_id_str in frame_results:
                        # Кадр уже обработан - пропускаем (идемпотентность)
                        print(f"[runner] Frame {frame_id} for event_id={event_id} already processed (idempotency check), skipping", flush=True)
                        return
                    
                    # ИСПРАВЛЕНИЕ (Пункт 4): Используем атомарную операцию для обновления с проверкой идемпотентности
                    # Это предотвращает race condition при параллельной обработке
                    result = runner_predictions.update_one(
                        {
                            "_id": event_id,
                            f"frame_results.{frame_id_str}": {"$exists": False}  # Условие для идемпотентности
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
                        # Кадр уже был обработан другим воркером или предыдущей итерацией
                        print(f"[runner] Frame {frame_id} for event_id={event_id} was already processed by another worker (idempotency check)", flush=True)
                        return
                    
                    # Получаем обновлённый документ для проверки завершённости
                    updated_doc = runner_predictions.find_one({"_id": event_id})
                    received_frames = updated_doc.get("received_frames", 0)
                    total_frames = updated_doc.get("total_frames", 0)
                    
                    print(f"[runner] Saved frame {frame_id} result for event_id={event_id} ({received_frames}/{total_frames} frames received)", flush=True)
                    
                    # Проверяем, все ли кадры получены
                    if received_frames >= total_frames:
                        print(f"[runner] All frames received for event_id={event_id}, merging results...", flush=True)
                        
                        # ИСПРАВЛЕНИЕ (Пункт 4): Проверяем, не был ли уже установлен статус "predictable" (идемпотентность)
                        final_doc = runner_predictions.find_one({"_id": event_id})
                        if final_doc.get("status") == "predictable":
                            print(f"[runner] Status already set to 'predictable' for event_id={event_id} (idempotency check)", flush=True)
                            return
                        
                        # Объединяем результаты кадров в правильном порядке
                        # Сортируем frame_id для сохранения очерёдности кадров
                        final_frame_results = final_doc.get("frame_results", {})
                        sorted_frame_ids = sorted([int(fid) for fid in final_frame_results.keys()])
                        
                        # Формируем финальный JSON в формате, совместимом со старой версией
                        final_prediction = []
                        for fid in sorted_frame_ids:
                            frame_result = final_frame_results[str(fid)]
                            # frame_result уже содержит структуру {"frame": frame_id, "objects": [...]}
                            final_prediction.append(frame_result)
                        
                        # ИСПРАВЛЕНИЕ (Пункт 4): Обновляем документ с финальным prediction ТОЛЬКО если статус ещё не "predictable"
                        # Статус "predictable" означает, что результат готов для сохранения в S3 через outbox_s3
                        update_result = runner_predictions.update_one(
                            {
                                "_id": event_id,
                                "status": {"$ne": "predictable"}  # Условие для идемпотентности
                            },
                            {
                                "$set": {
                                    "prediction": final_prediction,  # Финальный объединённый JSON
                                    "status": "predictable"  # Готов для сохранения в S3 через outbox_s3
                                }
                            }
                        )
                        
                        if update_result.modified_count == 0:
                            print(f"[runner] Status was already set to 'predictable' for event_id={event_id} (idempotency check)", flush=True)
                        else:
                            print(f"[runner] Merged {len(final_prediction)} frame results for event_id={event_id}, status set to 'predictable' in runner_db", flush=True)
                
                await loop.run_in_executor(None, save_frame_and_check_complete)
                
                # Коммитим сообщение только после успешной обработки результата
                await consumer.commit()
                print(f"[runner] Committed inference result message for event_id={event_id}, frame_id={frame_id}", flush=True)
                    
            except Exception as e:
                print(f"[runner] Error processing inference result: {e}", flush=True)
                import traceback
                traceback.print_exc()
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    """Запускаем Kafka consumers при старте сервиса"""
    asyncio.create_task(consume())  # Consumer для предобработки видео
    asyncio.create_task(consume_inference_results())  # Consumer для сохранения результатов в S3

@app.get("/health")
def health():
    return {"status": "ok"}
