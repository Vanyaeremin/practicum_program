"""
Inference Service — потребитель задач YOLO-инференса для обработки отдельных кадров

НОВАЯ АРХИТЕКТУРА (обработка по кадрам):
Сервис делает:
1. Читает задачи из Kafka темы "inference_tasks" (предобработанные кадры)
2. Получает кадр в формате base64 (JPEG) из сообщения Kafka (НЕ скачивает из MinIO)
3. Декодирует кадр и запускает YOLO детекцию на одном кадре
4. Отправляет JSON результат для кадра в Kafka тему "inference_results"
5. Обновляет MongoDB статусами обработки

ОЧЕРЁДНОСТЬ КАДРОВ:
Обеспечивается через Kafka key-based partitioning и consumer groups. Все кадры одного видео
отправляются с key=event_id.encode(), что гарантирует попадание всех сообщений в одну partition.
Kafka гарантирует последовательную обработку сообщений из одной partition в рамках одного consumer group.
Каждое сообщение содержит frame_id, который используется для правильной сборки финального JSON в runner.

ПРОИЗВОДИТЕЛЬНОСТЬ:
- Используется ProcessPoolExecutor для CPU-интенсивных YOLO операций
- Каждый процесс имеет свой GIL, что обеспечивает истинный параллелизм
- Модель YOLO загружается в каждом процессе отдельно (кэшируется внутри процесса)
- Это позволяет обрабатывать несколько кадров параллельно на многоядерных CPU

НАСТРОЙКА МОДЕЛИ:
- Файл модели: 'best.pt' (веса YOLO)
- Путь к модели задаётся в:
  * yolo_detect_frame.py: get_yolo_predictions_for_frame_base64(yolo_path='best.pt')
  * В этом файле: yolo_path='best.pt'

Чтобы сменить модель, обновите параметр 'best.pt' в указанных местах.
Файл модели нужно положить в директорию inference.
"""
import asyncio
import json
import os
from concurrent.futures import ProcessPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@mongo:27017/")

# Глобальный ProcessPoolExecutor для CPU-интенсивных YOLO операций
# Каждый процесс имеет свой GIL, что обеспечивает истинный параллелизм
# Модель YOLO загружается в каждом процессе отдельно (кэшируется внутри процесса)
executor = ProcessPoolExecutor(max_workers=os.cpu_count())

async def consume_inference():
    """
    Чтение задач инференса из Kafka и запуск YOLO на отдельных кадрах.
    
    НОВАЯ ЛОГИКА:
    1. Получает сообщение с кадром (base64-кодированный JPEG) из Kafka
    2. Декодирует кадр и запускает YOLO на одном кадре
    3. Отправляет JSON результат для кадра обратно в Kafka
    
    ОЧЕРЁДНОСТЬ КАДРОВ:
Обеспечивается через Kafka key-based partitioning. Все кадры одного видео обрабатываются
последовательно в рамках одной partition благодаря key=event_id.encode().
Каждое сообщение содержит frame_id, который сохраняется в результате для правильной 
сборки финального JSON в runner.
    """
    # Ожидаем готовности Kafka для consumer
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "inference_tasks",  # Тема Kafka с задачами на инференс (теперь кадры, а не видео)
                bootstrap_servers="kafka:9092",
                group_id="inference-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # Отключаем авто-коммит для надежности
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[inference] aiokafka consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[inference] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
    
    # Инициализируем producer для отправки результатов
    producer = None
    while producer is None:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            )
            await producer.start()
            print("[inference] aiokafka producer connected", flush=True)
        except Exception as e:
            print(f"[inference] aiokafka producer not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
    
    try:
        async for msg in consumer:
            try:
                # Достаём данные о кадре из сообщения Kafka
                event_id = msg.value.get("event_id")
                frame_id = msg.value.get("frame_id")  # Номер кадра в исходном видео
                bucket = msg.value.get("bucket")  # Обычно "videos"
                frame_data = msg.value.get("frame_data")  # Base64-кодированный JPEG кадр
                media_type = msg.value.get("media_type", "frame")  # "frame" для кадров
                
                if not event_id or frame_id is None or not bucket or not frame_data:
                    print(f"[inference] Invalid message format: {msg.value}", flush=True)
                    continue
                
                # Проверяем, что это кадр (не старое сообщение с видео)
                if media_type != "frame":
                    print(f"[inference] Skipping non-frame message (media_type={media_type})", flush=True)
                    continue
                
                print(f"[inference] Processing frame {frame_id} for event_id={event_id}")
                
                # Обрабатываем кадр через YOLO
                # Импортируем функцию для обработки кадра
                from yolo_detect_frame import get_yolo_predictions_for_frame_base64
                
                # Запускаем YOLO на кадре через ProcessPoolExecutor
                # MODEL USAGE: 'best.pt' — путь к модели
                # conf=0.25 — более низкий порог для лучшего распознавания животных
                # iou=0.45 — более строгий IoU для лучшей фильтрации дубликатов
                # ProcessPoolExecutor обеспечивает истинный параллелизм для CPU-интенсивных YOLO операций
                # Каждый процесс имеет свой GIL, что позволяет использовать все ядра CPU
                loop = asyncio.get_running_loop()
                frame_result = await loop.run_in_executor(
                    executor,  # ProcessPoolExecutor для обхода GIL и истинного параллелизма
                    get_yolo_predictions_for_frame_base64,
                    frame_data, frame_id
                )
                
                print(f"[inference] YOLO prediction completed for frame {frame_id} of event_id={event_id}")
                
                # Отправляем JSON результат для кадра в Kafka тему "inference_results"
                try:
                    message = {
                        "event_id": event_id,
                        "frame_id": frame_id,  # Номер кадра для правильной сборки в runner
                        "bucket": bucket,
                        "prediction": frame_result  # JSON результат для одного кадра
                    }
                    # Используем event_id как ключ для гарантии, что все результаты одного видео попадут в одну partition
                    key = event_id.encode() if event_id else None
                    await producer.send_and_wait("inference_results", message, key=key)
                    print(f"[inference] Sent frame {frame_id} prediction to Kafka topic 'inference_results' for event_id={event_id}, key={key}")
                    
                    # Коммитим сообщение только после успешной обработки и отправки результата
                    await consumer.commit()
                    print(f"[inference] Committed frame {frame_id} processing for event_id={event_id}", flush=True)
                    
                except Exception as e:
                    print(f"[inference] Failed to send frame prediction to Kafka: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
                
                print(f"[inference] Completed YOLO prediction for frame {frame_id} of event_id={event_id}")
            
            except Exception as e:
                print(f"[inference] Error processing frame: {e}", flush=True)
                import traceback
                traceback.print_exc()
    
    finally:
        await consumer.stop()
        if producer:
            await producer.stop()

def run_bg_consumer():
    """Запускает Kafka consumer в фоне при старте сервиса"""
    loop = asyncio.get_event_loop()
    loop.create_task(consume_inference())

