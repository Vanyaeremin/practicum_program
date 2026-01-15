"""
Inference Service — потребитель задач YOLO-инференса для обработки отдельных кадров

Архитектура обработки по кадрам:
1. Читает задачи из Kafka темы "inference_tasks" (пути к кадрам в S3)
2. Получает путь к кадру в S3 из сообщения Kafka
3. Скачивает кадр из S3 бакета "frames" и запускает YOLO детекцию на одном кадре
4. Отправляет JSON результат для кадра в Kafka тему "inference_results"

Очерёдность кадров:
Обеспечивается через Kafka key-based partitioning и consumer groups. Все кадры одного видео
отправляются с key=event_id.encode(), что гарантирует попадание всех сообщений в одну partition.
Kafka гарантирует последовательную обработку сообщений из одной partition в рамках одного
consumer group.
Каждое сообщение содержит frame_id, который используется для правильной сборки
финального JSON в runner.

Производительность:
- Используется ProcessPoolExecutor для CPU-интенсивных YOLO операций
- Каждый процесс имеет свой GIL, что обеспечивает истинный параллелизм
- Модель YOLO загружается в каждом процессе отдельно (кэшируется внутри процесса)
- Это позволяет обрабатывать несколько кадров параллельно на многоядерных CPU

Настройка модели:
- Файл модели: 'best.pt' (веса YOLO)
- Путь к модели задаётся в yolo_detect_frame.py: get_yolo_predictions_for_frame(yolo_path='best.pt')
- Чтобы сменить модель, обновите параметр 'best.pt' в указанных местах
- Файл модели нужно положить в директорию inference
"""
import asyncio
import json
import os
import traceback
from concurrent.futures import ProcessPoolExecutor

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from s3_utils import check_s3_availability, download_frame
from yolo_detect_frame import get_yolo_predictions_for_frame

executor = ProcessPoolExecutor(max_workers=os.cpu_count())

async def consume_inference():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "inference_tasks",  
                bootstrap_servers="kafka:9092",
                group_id="inference-group",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            await consumer.start()
            print("[inference] aiokafka consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[inference] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)

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
                event_id = msg.value.get("event_id")
                frame_id = msg.value.get("frame_id")  
                bucket = msg.value.get("bucket")  
                frame_path = msg.value.get(
                    "frame_path"
                )  
                media_type = msg.value.get(
                    "media_type", "frame"
                )  

                if not event_id or frame_id is None or not bucket or not frame_path:
                    print(f"[inference] Invalid message format: {msg.value}", flush=True)
                    continue

                if media_type != "frame":
                    print(
                        f"[inference] Skipping non-frame message (media_type={media_type})",
                        flush=True
                    )
                    continue

                print(
                    f"[inference] Processing frame {frame_id} for event_id={event_id}, "
                    f"path: {frame_path}"
                )

                loop = asyncio.get_running_loop()
                s3_available = False
                while not s3_available:
                    s3_available = await loop.run_in_executor(
                        None, check_s3_availability, "inference"
                    )
                    if not s3_available:
                        print(
                            "[inference] S3/MinIO not ready yet, retry in 5s...",
                            flush=True
                        )
                        await asyncio.sleep(5)
                frame_bytes = None
                download_attempts = 0
                max_download_attempts = 3

                while frame_bytes is None and download_attempts < max_download_attempts:
                    try:
                        frame_bytes = await loop.run_in_executor(
                            None,  
                            download_frame,
                            bucket,
                            frame_path
                        )
                        print(
                            f"[inference] Downloaded frame {frame_id} from S3: "
                            f"{len(frame_bytes)} bytes"
                        )
                    except Exception as download_error:
                        download_attempts += 1
                        if download_attempts < max_download_attempts:
                            print(
                                f"[inference] Failed to download frame {frame_id} from S3 "
                                f"(attempt {download_attempts}/{max_download_attempts}): "
                                f"{download_error}, retry in 5s...",
                                flush=True
                            )
                            await asyncio.sleep(5)
                            s3_available = await loop.run_in_executor(
                                None, check_s3_availability, "inference"
                            )
                            if not s3_available:
                                print(
                                    "[inference] S3/MinIO not available, waiting...",
                                    flush=True
                                )
                                await asyncio.sleep(5)
                        else:
                            print(
                                f"[inference] Failed to download frame {frame_id} from S3 "
                                f"after {max_download_attempts} attempts: {download_error}",
                                flush=True
                            )
                            raise
                if frame_bytes is None:
                    raise Exception(
                        f"Failed to download frame {frame_id} from S3 "
                        f"after {max_download_attempts} attempts"
                    )

                frame_result = await loop.run_in_executor(
                    executor,  
                    get_yolo_predictions_for_frame,
                    frame_bytes,
                    frame_id
                )

                print(
                    f"[inference] YOLO prediction completed for frame {frame_id} "
                    f"of event_id={event_id}"
                )

                try:
                    message = {
                        "event_id": event_id,
                        "frame_id": frame_id,  
                        "bucket": bucket,
                        "prediction": frame_result  
                    }
                    key = event_id.encode() if event_id else None
                    await producer.send_and_wait("inference_results", message, key=key)
                    print(
                        f"[inference] Sent frame {frame_id} prediction to Kafka topic "
                        f"'inference_results' for event_id={event_id}, key={key}"
                    )

                    await consumer.commit()
                    print(
                        f"[inference] Committed frame {frame_id} processing for "
                        f"event_id={event_id}",
                        flush=True
                    )

                except Exception as e:
                    print(
                        f"[inference] Failed to send frame prediction to Kafka: {e}",
                        flush=True
                    )
                    traceback.print_exc()

                print(
                    f"[inference] Completed YOLO prediction for frame {frame_id} "
                    f"of event_id={event_id}"
                )

            except Exception as e:
                print(f"[inference] Error processing frame: {e}", flush=True)
                traceback.print_exc()

    finally:
        await consumer.stop()
        if producer:
            await producer.stop()


def run_bg_consumer():
    loop = asyncio.get_event_loop()
    loop.create_task(consume_inference())