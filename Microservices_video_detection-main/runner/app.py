"""
Runner Service — сервис предобработки видео

НОВАЯ АРХИТЕКТУРА (микросервисная с отдельной БД):
1. Читает события из Kafka темы "outbox_events"
2. Скачивает видео из MinIO (input/{video_id}.mp4)
3. Делит видео на кадры (извлекает каждый 5-й кадр)
4. Предобрабатывает каждый кадр отдельно (Gaussian blur + корректировка контраста)
5. Отправляет каждый предобработанный кадр в inference через Kafka тему "inference_tasks"
6. Читает результаты инференса по кадрам из Kafka темы "inference_results"
7. Сохраняет JSON по кадрам в отдельную БД runner_db.runner_predictions
8. Когда все кадры обработаны, объединяет JSON и устанавливает статус "predictable"
9. Outbox S3 worker забирает данные из runner_db при статусе "have ready result" и сохраняет в S3

ВАЖНО:
- Предобработанное видео НЕ сохраняется в MinIO
- Каждый кадр обрабатывается и отправляется в inference отдельно
- Очерёдность кадров обеспечивается через consumer groups в Kafka
- Все операции полностью асинхронные
- Используется отдельная БД runner_db для поддержания микросервисной архитектуры
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

def update_outbox_status(event_id, status):
    """Обновляет статус в MongoDB outbox"""
    client = MongoClient(MONGO_URI)
    db = client['main_db']
    outbox = db['outbox']
    outbox.update_one({"_id": event_id}, {"$set": {"status": status}})
    client.close()

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

async def consume():
    """
    Чтение событий обработки видео из Kafka и отправка кадров в inference.
    
    НОВАЯ ЛОГИКА:
    1. Скачивает видео из MinIO
    2. Извлекает каждый 5-й кадр и предобрабатывает его
    3. Отправляет каждый кадр в inference через Kafka
    4. НЕ сохраняет предобработанное видео в MinIO
    
    ОЧЕРЁДНОСТЬ КАДРОВ:
    Обеспечивается через несколько механизмов:
    
    1. Kafka Partitioning с ключом: Все кадры одного видео отправляются с key=event_id.encode(),
       что гарантирует попадание всех сообщений одного video_id в одну partition.
    
    2. Kafka Consumer Groups: Все кадры одного видео обрабатываются в рамках одного
       consumer group ("inference-group"). Kafka гарантирует, что сообщения из одной
       partition обрабатываются последовательно одним consumer в группе.
    
    3. Последовательная отправка: Кадры отправляются в Kafka последовательно
       (в цикле for), что сохраняет порядок отправки.
    
    4. frame_id в сообщениях: Каждое сообщение содержит frame_id (номер кадра
       в исходном видео: 0, 5, 10, 15, ...). Этот ID используется для правильной
       сборки финального JSON в правильном порядке.
    
    5. Сортировка при объединении: В consume_inference_results() кадры сортируются
       по frame_id перед объединением в финальный JSON, что гарантирует правильный
       порядок даже если кадры пришли в другом порядке (хотя это маловероятно
       благодаря key-based partitioning).
    
    Подробное объяснение см. в файле FRAME_ORDERING.md
    """
    # Ожидаем готовности Kafka
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "outbox_events",  # Тема Kafka с заданиями на обработку видео
                bootstrap_servers="kafka:9092",
                group_id="runner-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Отключаем авто-коммит для надежности
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[runner] aiokafka consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[runner] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
    print("[runner] Starting to iterate over messages (async)...", flush=True)
    try:
        from s3_utils import download_image
        from async_wrapper import preprocess_image_async
        from frame_extractor import extract_and_preprocess_frames_async
        from kafka_producer import RunnerKafkaProducer
        
        async for msg in consumer:
            try:
                # Достаём данные о видео из сообщения Kafka
                payload = msg.value.get("payload", {})
                bucket = payload.get('bucket')  # Обычно "videos"
                object_name = payload.get('object')  # Путь: input/{video_id}.mp4
                media_type = payload.get('media_type', 'image')  # image|video
                
                if bucket and object_name:
                    event_id = object_name.split('/')[-1].replace('.mp4', '').replace('.jpg', '').replace('.jpeg', '')
                    
                    # ПРОВЕРКА ДОСТУПНОСТИ S3 ПЕРЕД ОБНОВЛЕНИЕМ СТАТУСА
                    # Ждём доступности S3/MinIO перед началом обработки
                    s3_available = False
                    while not s3_available:
                        loop = asyncio.get_running_loop()
                        s3_available = await loop.run_in_executor(None, check_s3_availability)
                        if not s3_available:
                            print(f"[runner] S3/MinIO not ready yet, retry in 5s...", flush=True)
                            await asyncio.sleep(5)
                    
                    print(f"[runner] S3/MinIO is available, proceeding with event_id={event_id}", flush=True)
                    
                    print(f"[runner] Downloading from S3: bucket={bucket}, object={object_name}, type={media_type}", flush=True)
                    
                    # Скачиваем файл из MinIO асинхронно
                    loop = asyncio.get_running_loop()
                    file_bytes = await loop.run_in_executor(None, download_image, bucket, object_name)
                    print(f"[runner] File downloaded from S3 ({len(file_bytes)} bytes)", flush=True)
                    
                    # Обновляем статус на "in progress" ТОЛЬКО ПОСЛЕ УСПЕШНОГО СКАЧИВАНИЯ видео
                    if event_id:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, update_outbox_status, event_id, "in progress")

                    # Обработка видео: извлекаем кадры и отправляем в inference
                    if media_type == 'video':
                        # Извлекаем каждый 5-й кадр и предобрабатываем его
                        # Функция возвращает список кортежей (frame_id, frame_bytes)
                        frames = await extract_and_preprocess_frames_async(
                            file_bytes,
                            every_n_frames=5,  # Каждый 5-й кадр
                            blur_ksize=3,
                            alpha=1.3,
                            beta=5
                        )
                        print(f"[runner] Extracted {len(frames)} frames from video {event_id}", flush=True)
                        
                        # ИНИЦИАЛИЗИРУЕМ СТРУКТУРУ ДЛЯ ОТСЛЕЖИВАНИЯ КАДРОВ ДО ОТПРАВКИ В INFERENCE
                        # Используем отдельную БД runner_db для поддержания архитектуры микросервисности
                        def init_frame_tracking():
                            db = get_runner_db()
                            runner_predictions = db['runner_predictions']
                            
                            # Создаём запись для отслеживания прогресса обработки кадров
                            runner_predictions.update_one(
                                {"_id": event_id},
                                {
                                    "$set": {
                                        "event_id": event_id,
                                        "bucket": bucket,
                                        "total_frames": len(frames),
                                        "received_frames": 0,
                                        "frame_results": {},  # Словарь {frame_id: json_result}
                                        "status": "processing"  # Обрабатывается
                                    }
                                },
                                upsert=True
                            )
                        
                        await loop.run_in_executor(None, init_frame_tracking)
                        print(f"[runner] Initialized frame tracking BEFORE sending frames for event_id={event_id}, total_frames={len(frames)}", flush=True)
                        
                        # Коммитим сообщение только после успешной инициализации трекинга
                        await consumer.commit()
                        print(f"[runner] Committed message for event_id={event_id}", flush=True)
                        
                        # Инициализируем producer для отправки кадров в inference
                        producer = RunnerKafkaProducer()
                        await producer.start()
                        
                        # Отправляем каждый кадр в inference через Kafka
                        # ВАЖНО: Отправляем кадры последовательно для сохранения очерёдности
                        # Kafka гарантирует порядок сообщений в рамках одного partition
                        import base64
                        loop = asyncio.get_running_loop()
                        
                        def encode_base64(data):
                            """Синхронная функция для кодирования в base64"""
                            return base64.b64encode(data).decode('utf-8')
                        
                        for frame_id, frame_bytes in frames:
                            # Кодируем frame_bytes в base64 для передачи через JSON (в executor для больших кадров)
                            frame_base64 = await loop.run_in_executor(None, encode_base64, frame_bytes)
                            
                            # Отправляем кадр в inference
                            await producer.send({
                                "event_id": event_id,
                                "frame_id": frame_id,  # Номер кадра для правильной сборки JSON
                                "bucket": bucket,
                                "frame_data": frame_base64,  # Base64-кодированный JPEG кадр
                                "media_type": "frame"  # Указываем, что это отдельный кадр
                            })
                            print(f"[runner] Sent frame {frame_id} to inference for event_id={event_id}", flush=True)
                        
                        await producer.stop()
                        print(f"[runner] All {len(frames)} frames sent to inference for event_id={event_id}", flush=True)
                    
                    else:
                        # Обработка изображений (старая логика без изменений)
                        processed_bytes = await preprocess_image_async(file_bytes)
                        print(f"[runner] Image processed! Bytes: {len(processed_bytes)}", flush=True)
                        
                        # Для изображений отправляем как раньше (если нужно)
                        # Здесь можно оставить старую логику или адаптировать под кадры
                        pass
                else:
                    print(f"[runner] No S3 info in message: {msg.value}", flush=True)
            except Exception as e:
                print(f"[runner] Error in runner message handling: {e}", flush=True)
                import traceback
                traceback.print_exc()
    finally:
        await consumer.stop()

async def consume_inference_results():
    """
    Чтение результатов инференса по кадрам из Kafka и сборка финального JSON.
    
    НОВАЯ ЛОГИКА (микросервисная архитектура):
    1. Получает JSON результат для одного кадра от inference
    2. Сохраняет результат кадра в runner_db.runner_predictions
    3. Проверяет, все ли кадры получены
    4. Когда все кадры получены, объединяет их в правильном порядке (по frame_id)
    5. Устанавливает статус "predictable" в runner_db и "have ready result" в main_db.outbox
    6. Outbox S3 worker забирает данные из runner_db и сохраняет в S3
    
    ОЧЕРЁДНОСТЬ КАДРОВ:
    Обеспечивается через frame_id в сообщениях Kafka и сортировку при объединении.
    Каждый кадр имеет уникальный frame_id, который соответствует его позиции в исходном видео.
    При объединении кадры сортируются по frame_id для сохранения правильного порядка.
    
    МИКРОСЕРВИТНАЯ АРХИТЕКТУРА:
    - runner_db используется для хранения результатов обработки кадров
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
                    """
                    # Используем отдельную БД runner_db для результатов
                    db = get_runner_db()
                    runner_predictions = db['runner_predictions']
                    
                    # Для обновления статуса в main_db.outbox
                    main_client = MongoClient(MONGO_URI)
                    main_db = main_client['main_db']
                    outbox = main_db['outbox']
                    
                    # Получаем текущее состояние обработки видео из runner_db
                    doc = runner_predictions.find_one({"_id": event_id})
                    
                    if not doc:
                        print(f"[runner] No tracking document found in runner_db for event_id={event_id}, skipping", flush=True)
                        main_client.close()
                        return
                    
                    # Обновляем результат для конкретного кадра
                    frame_results = doc.get("frame_results", {})
                    frame_results[str(frame_id)] = frame_prediction  # Сохраняем как строку для JSON
                    
                    received_frames = doc.get("received_frames", 0) + 1
                    total_frames = doc.get("total_frames", 0)
                    
                    # Обновляем документ с новым результатом кадра в runner_db
                    runner_predictions.update_one(
                        {"_id": event_id},
                        {
                            "$set": {
                                "frame_results": frame_results,
                                "received_frames": received_frames
                            }
                        }
                    )
                    
                    print(f"[runner] Saved frame {frame_id} result for event_id={event_id} ({received_frames}/{total_frames} frames received)", flush=True)
                    
                    # Проверяем, все ли кадры получены
                    if received_frames >= total_frames:
                        print(f"[runner] All frames received for event_id={event_id}, merging results...", flush=True)
                        
                        # Объединяем результаты кадров в правильном порядке
                        # Сортируем frame_id для сохранения очерёдности кадров
                        sorted_frame_ids = sorted([int(fid) for fid in frame_results.keys()])
                        
                        # Формируем финальный JSON в формате, совместимом со старой версией
                        final_prediction = []
                        for fid in sorted_frame_ids:
                            frame_result = frame_results[str(fid)]
                            # frame_result уже содержит структуру {"frame": frame_id, "objects": [...]}
                            final_prediction.append(frame_result)
                        
                        # Обновляем документ с финальным prediction в runner_db
                        runner_predictions.update_one(
                            {"_id": event_id},
                            {
                                "$set": {
                                    "prediction": final_prediction,  # Финальный объединённый JSON
                                    "status": "predictable"  # Готов для сохранения в S3 через outbox_s3
                                }
                            }
                        )
                        
                        # Обновляем статус в outbox на "have ready result"
                        outbox.update_one(
                            {"_id": event_id},
                            {
                                "$set": {
                                    "status": "have ready result"
                                }
                            }
                        )
                        main_client.close()
                        print(f"[runner] Merged {len(final_prediction)} frame results for event_id={event_id}, status set to 'predictable'", flush=True)
                
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
