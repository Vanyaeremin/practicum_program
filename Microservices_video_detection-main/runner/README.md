# Runner Service (Порт: 8002)

## Обзор

**Runner Service** — сервис предобработки видео в микросервисной архитектуре детекции объектов. Сервис отвечает за извлечение кадров из видео, их предобработку и отправку в inference сервис для детекции объектов.

## Основные функции

1. **Извлечение кадров** из видео (каждый 5-й кадр)
2. **Предобработка кадров** (Gaussian blur + корректировка контраста)
3. **Отправка кадров** в inference сервис через Kafka
4. **Сбор результатов** детекции от inference сервиса
5. **Объединение результатов** в финальный JSON
6. **Обновление статусов** обработки в MongoDB

## Архитектура обработки

### НОВАЯ АРХИТЕКТУРА (микросервисная с отдельной БД)

Сервис работает по принципу обработки видео покадерно с использованием отдельной базы данных:

1. **Читает события** из Kafka темы `outbox_events`
2. **Скачивает видео** из MinIO по пути `input/{video_id}.mp4`
3. **Извлекает кадры** (каждый 5-й кадр) с помощью OpenCV
4. **Предобрабатывает** каждый кадр отдельно:
   - Gaussian blur (ksize=3)
   - Корректировка контраста/яркости (alpha=1.3, beta=5)
5. **Отправляет кадры** в inference через Kafka тему `inference_tasks`
6. **Получает результаты** из Kafka темы `inference_results`
7. **Сохраняет результаты** в отдельную БД `runner_db.runner_predictions`
8. **Объединяет кадры** в финальный JSON при получении всех результатов
9. **Устанавливает статус** `predictable` в runner_db (НЕ обновляет outbox - соответствует принципам микросервисов)

## API Эндпоинты

### Health Check
- `GET /health` — Health check сервиса

## Внутренние компоненты

### Kafka Consumers

#### 1. Consumer для предобработки видео
- **Тема**: `outbox_events`
- **Группа**: `runner-group`
- **Функция**: Извлечение и отправка кадров в inference

#### 2. Consumer для сборки результатов
- **Тема**: `inference_results`
- **Группа**: `runner-results-group`
- **Функция**: Сбор результатов детекции по кадрам

### Очерёдность кадров

Очерёдность кадров обеспечивается несколькими механизмами:

1. **Kafka Consumer Groups**: Все кадры одного видео обрабатываются в рамках одного consumer group, что гарантирует последовательную обработку
2. **Последовательная отправка**: Кадры отправляются в Kafka последовательно в цикле
3. **frame_id**: Каждое сообщение содержит `frame_id` (номер кадра: 0, 5, 10, 15, ...)
4. **Сортировка при объединении**: Кадры сортируются по `frame_id` перед объединением в финальный JSON

## Технические детали

### Зависимости
- **FastAPI** — веб-фреймворк
- **aiokafka** — асинхронный клиент Kafka
- **MongoDB** — хранилище статусов и промежуточных результатов
- **OpenCV** — обработка видео и извлечение кадров
- **NumPy** — операции с массивами данных

### Конфигурация
Переменные окружения:
- `MONGO_URI` — строка подключения к основной MongoDB (по умолчанию: `mongodb://admin:password@mongo:27017/`)
- `RUNNER_MONGO_URI` — строка подключения к runner_db (по умолчанию: `mongodb://admin:password@mongo:27017/`)

### Микросервисная архитектура БД

#### main_db
- **outbox** — события для обработки и их статусы (используется для обновления статусов)

#### runner_db
- **runner_predictions** — результаты обработки кадров от сервиса runner
- Данные остаются в БД после сохранения в S3 (для истории и отладки)

#### Преимущества разделения
1. **Независимость сервисов** — runner имеет свою собственную БД
2. **Изоляция данных** — проблемы в runner не влияют на другие сервисы
3. **Масштабируемость** — БД можно масштабировать независимо
4. **История данных** — результаты сохраняются для аналитики

### Асинхронность и избегание блокировки Event Loop

Сервис активно использует асинхронность для обеспечения высокой производительности и отзывчивости:

#### 1. Асинхронные клиенты Kafka (aiokafka)

**Проблема**: Синхронный клиент Kafka блокирует event loop при отправке/получении сообщений.

**Решение**: Использование `aiokafka` — полностью асинхронного клиента.

```python
# runner/app.py — Consumer для получения задач
from aiokafka import AIOKafkaConsumer

consumer = AIOKafkaConsumer(
    "outbox_events",
    bootstrap_servers="kafka:9092",
    group_id="runner-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # Ручной коммит для надёжности
    value_deserializer=lambda m: json.loads(m.decode()),
)

await consumer.start()  # Асинхронное подключение, не блокирует event loop

# Асинхронная итерация по сообщениям
async for msg in consumer:
    # Event loop не блокируется во время ожидания новых сообщений
    await process_message(msg)
    await consumer.commit()  # Асинхронный коммит
```

```python
# runner/kafka_producer.py — Producer для отправки кадров
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

await producer.start()  # Асинхронное подключение

# Асинхронная отправка с ожиданием подтверждения
await producer.send_and_wait("inference_tasks", message, key=key)
# Event loop не блокируется, может обрабатывать другие задачи
```

**Преимущества**:
- Event loop не блокируется
- Можно обрабатывать множество сообщений одновременно
- Высокая производительность

#### 2. run_in_executor для I/O-блокирующих операций

**Проблема**: Синхронные операции с MongoDB и MinIO блокируют event loop.

**Решение**: Использование `run_in_executor` для вынесения блокирующих операций в отдельный поток.

```python
# runner/app.py — скачивание файла из MinIO
loop = asyncio.get_running_loop()
# download_image — синхронная функция из s3_utils.py
# Выполняется в отдельном потоке, event loop не блокируется
file_bytes = await loop.run_in_executor(None, download_image, bucket, object_name)
```

```python
# runner/app.py — инициализация трекинга в runner_db
def init_frame_tracking():
    """Синхронная функция для инициализации отслеживания кадров"""
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
                "status": "processing"  # Внутренний статус в runner_db
            }
        },
        upsert=True
    )

loop = asyncio.get_running_loop()
# Выполняется в отдельном потоке, event loop не блокируется
await loop.run_in_executor(None, init_frame_tracking)
```

**ВАЖНО**: Runner НЕ обновляет outbox - только runner_db. Это соответствует принципам микросервисной архитектуры.

**Как это работает**:
- `run_in_executor(None, func)` использует `ThreadPoolExecutor` по умолчанию
- Функция выполняется в отдельном потоке
- Event loop продолжает обрабатывать другие задачи
- Когда функция завершается, результат возвращается через await

#### 3. ProcessPoolExecutor для CPU-интенсивных задач (обход GIL)

**Проблема**: CPU-интенсивные операции (обработка изображений, извлечение кадров) блокируют GIL и event loop.

**Решение**: Использование `ProcessPoolExecutor` для выполнения CPU-bound задач в отдельных процессах.

```python
# runner/async_wrapper.py
import os
from concurrent.futures import ProcessPoolExecutor

# Создаём ProcessPoolExecutor с количеством воркеров = количество CPU ядер
# Каждый процесс имеет свой собственный GIL, что позволяет использовать все ядра CPU
executor = ProcessPoolExecutor(max_workers=os.cpu_count())

async def preprocess_image_async(img_bytes: bytes) -> bytes:
    """
    Асинхронная обёртка для предобработки изображений.
    
    Выполняет синхронную функцию preprocess_image в отдельном ПРОЦЕССЕ
    через ProcessPoolExecutor, что позволяет:
    1. Не блокировать event loop
    2. Использовать все ядра CPU (истинный параллелизм)
    3. Обойти ограничения GIL
    """
    loop = asyncio.get_running_loop()
    # executor (ProcessPoolExecutor) вместо None (ThreadPoolExecutor)
    return await loop.run_in_executor(
        executor,  # ProcessPoolExecutor
        preprocess_image,  # CPU-интенсивная функция из image_cpu.py
        img_bytes
    )
```

```python
# runner/frame_extractor.py
executor_frames = ProcessPoolExecutor(max_workers=os.cpu_count())

async def extract_and_preprocess_frames_async(video_bytes: bytes, ...):
    """
    Извлечение кадров из видео — CPU-интенсивная операция.
    Выполняется в отдельном процессе для использования всех ядер CPU.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        executor_frames,  # ProcessPoolExecutor
        extract_and_preprocess_frames_sync,  # CPU-bound функция
        video_bytes,
        every_n_frames,
        blur_ksize,
        alpha,
        beta
    )
```

**Как это работает**:
- `ProcessPoolExecutor` создаёт отдельные процессы Python
- Каждый процесс имеет свой собственный GIL
- Несколько процессов могут выполнять CPU-интенсивный код одновременно на разных ядрах CPU
- Event loop не блокируется, так как выполнение происходит в отдельном процессе

**Преимущества ProcessPoolExecutor**:
- **Истинный параллелизм**: Используются все ядра CPU
- **Обход GIL**: Каждый процесс имеет свой GIL
- **Не блокирует event loop**: Выполнение в отдельном процессе
- **Масштабируемость**: Можно обрабатывать несколько видео одновременно

### Борьба с GIL (Global Interpreter Lock)

**GIL (Global Interpreter Lock)** — это механизм в CPython, который предотвращает одновременное выполнение байт-кода несколькими потоками в одном процессе. Это блокирует истинный параллелизм для CPU-интенсивных задач.

**Проблема в Runner Service**:
- Извлечение кадров из видео — CPU-интенсивная операция
- Предобработка изображений (Gaussian blur, корректировка контраста) — CPU-интенсивная операция
- Если выполнять эти операции в основном потоке, GIL блокирует выполнение других задач

**Решение**:
1. **ProcessPoolExecutor** для CPU-интенсивных задач:
   - Каждый процесс имеет свой GIL
   - Истинный параллелизм на многоядерных CPU
   - Event loop не блокируется

2. **ThreadPoolExecutor** (через `run_in_executor(None, ...)`) для I/O операций:
   - MongoDB операции
   - MinIO операции
   - Event loop не блокируется

**Пример использования**:
```python
# CPU-интенсивная задача — используем ProcessPoolExecutor
frames = await extract_and_preprocess_frames_async(video_bytes, every_n_frames=5)

# I/O операция — используем ThreadPoolExecutor (None)
file_bytes = await loop.run_in_executor(None, download_image, bucket, object_name)
```

## Процесс обработки видео

### 1. Получение задачи
```python
# Чтение из Kafka темы outbox_events
payload = msg.value.get("payload", {})
bucket = payload.get('bucket')  # "videos"
object_name = payload.get('object')  # "input/{video_id}.mp4"
```

### 2. Извлечение кадров
```python
frames = await extract_and_preprocess_frames_async(
    file_bytes,
    every_n_frames=5,  # Каждый 5-й кадр
    blur_ksize=3,
    alpha=1.3,
    beta=5
)
```

### 3. Отправка в inference
```python
for frame_id, frame_bytes in frames:
    frame_base64 = base64.b64encode(frame_bytes).decode('utf-8')
    await producer.send({
        "event_id": event_id,
        "frame_id": frame_id,
        "bucket": bucket,
        "frame_data": frame_base64,
        "media_type": "frame"
    })
```

### 4. Сбор результатов
```python
# Сохранение результатов кадров в runner_db.runner_predictions
runner_predictions.update_one(
    {"_id": event_id},
    {
        "$set": {
            "frame_results": frame_results,
            "received_frames": received_frames
        }
    }
)

# При завершении обновляем статус только в runner_db
# В runner_db: status = "predictable" (готов к сохранению в S3)
# ВАЖНО: Runner НЕ обновляет outbox - это соответствует принципам микросервисов
```

## Управление статусами

Сервис обновляет статусы в обеих БД:

### В runner_db.runner_predictions (внутренние статусы):
- `processing` — идёт обработка кадров
- `predictable` — все кадры обработаны, результат готов для сохранения в S3 через outbox_s3
- `ready` — результат сохранён в S3 (обновляется outbox_s3 worker)

**ВАЖНО**: Runner НЕ обновляет outbox (main_db.outbox) - это соответствует принципам микросервисной архитектуры. Outbox используется только для паттерна Transactional Outbox (статусы "new" и "sent").

## Отказоустойчивость и проверка доступности сервисов

### Проверка доступности Kafka

Сервис проверяет доступность Kafka при старте и автоматически восстанавливается при сбоях:

```python
# runner/app.py — подключение к Kafka с повторными попытками
async def consume():
    """Чтение событий из Kafka с проверкой доступности"""
    # Ожидаем готовности Kafka
    while True:
        try:
            consumer = AIOKafkaConsumer(...)
            await consumer.start()
            print("[runner] aiokafka consumer connected", flush=True)
            break  # Успешное подключение
        except Exception as e:
            print(f"[runner] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)  # Повторная попытка через 5 секунд
    
    # Основной цикл обработки сообщений
    try:
        async for msg in consumer:
            # Обработка сообщения...
            await consumer.commit()  # Коммит только после успешной обработки
    finally:
        await consumer.stop()  # Корректное закрытие при завершении
```

**Преимущества**:
- **Автоматическое восстановление**: При восстановлении Kafka сервис автоматически подключается
- **Не блокирует старт**: Сервис запускается и ждёт доступности Kafka
- **Логирование**: Все ошибки подключения логируются

### Проверка доступности MinIO/S3

Сервис проверяет доступность MinIO перед началом обработки видео:

```python
# runner/app.py — проверка доступности S3
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
            # Попытка пересоздать клиент
            from s3_utils import recreate_client
            new_client = recreate_client()
            new_client.list_buckets()
            return True
        except Exception as reinit_error:
            print(f"[runner] Reinitialization failed: {reinit_error}", flush=True)
            return False

# Использование в async функции
async def consume():
    async for msg in consumer:
        # ПРОВЕРКА ДОСТУПНОСТИ S3 ПЕРЕД ОБНОВЛЕНИЕМ СТАТУСА
        s3_available = False
        while not s3_available:
            loop = asyncio.get_running_loop()
            s3_available = await loop.run_in_executor(None, check_s3_availability)
            if not s3_available:
                print(f"[runner] S3/MinIO not ready yet, retry in 5s...", flush=True)
                await asyncio.sleep(5)  # Повторная попытка через 5 секунд
        
        print(f"[runner] S3/MinIO is available, proceeding...")
        # Продолжаем обработку...
```

**Преимущества**:
- **Graceful degradation**: Сервис ждёт доступности S3 вместо падения
- **Автоматическое восстановление**: При восстановлении S3 обработка продолжается автоматически
- **Логирование**: Все ошибки логируются для мониторинга

### Проверка доступности MongoDB

Сервис обрабатывает ошибки MongoDB с логированием и продолжением работы:

```python
# runner/app.py — обновление статуса в MongoDB
def update_outbox_status(event_id, status):
    """Обновляет статус в MongoDB outbox с обработкой ошибок"""
    try:
        client = MongoClient(MONGO_URI)
        db = client['main_db']
        outbox = db['outbox']
        outbox.update_one({"_id": event_id}, {"$set": {"status": status}})
        client.close()
    except Exception as e:
        print(f"[runner] MongoDB error updating outbox status: {e}", flush=True)
        # При ошибке статус не обновляется, но обработка продолжается
        # Событие будет обработано при следующей попытке

# Использование в async функции
loop = asyncio.get_running_loop()
await loop.run_in_executor(None, init_frame_tracking)  # Инициализация трекинга в runner_db
```

```python
# runner/app.py — сохранение результатов в runner_db
def save_frame_and_check_complete():
    """Сохраняет результат кадра в runner_db и проверяет завершение обработки"""
    try:
        # Используем отдельную БД runner_db для микросервисной архитектуры
        db = get_runner_db()
        runner_predictions = db['runner_predictions']
        
        # Сохраняем результат кадра
        runner_predictions.update_one(
            {"_id": event_id},
            {
                "$set": {
                    "frame_results": frame_results,
                    "received_frames": received_frames
                }
            }
        )
        
        # Если все кадры получены, устанавливаем статус "predictable"
        if received_frames >= total_frames:
            runner_predictions.update_one(
                {"_id": event_id},
                {
                    "$set": {
                        "prediction": final_prediction,
                        "status": "predictable"  # Готов к сохранению в S3
                    }
                }
            )
        
    except Exception as e:
        print(f"[runner] MongoDB error saving frame result: {e}", flush=True)
        # При ошибке результат не сохраняется, но обработка продолжается
        # Kafka сообщение не коммитится, будет обработано повторно
```

**ВАЖНО**: Runner НЕ обновляет outbox - только runner_db. Это соответствует принципам микросервисной архитектуры.

**Преимущества**:
- **Обработка ошибок**: Все ошибки MongoDB логируются
- **Продолжение работы**: Сервис продолжает работать при временных сбоях БД
- **Повторная обработка**: Kafka сообщения не коммитятся при ошибках, будут обработаны повторно

## Обработка ошибок

Сервис обрабатывает следующие типы ошибок с детальным логированием:

### Ошибки подключения к Kafka
- **Симптомы**: Не удаётся подключиться к Kafka брокеру
- **Обработка**: Повторные попытки каждые 5 секунд с логированием
- **Восстановление**: Автоматическое подключение при восстановлении Kafka

### Ошибки скачивания файлов из MinIO
- **Симптомы**: Не удаётся скачать видео из MinIO
- **Обработка**: Проверка доступности перед обработкой, повторные попытки
- **Восстановление**: Автоматическое восстановление при восстановлении MinIO

### Ошибки извлечения кадров из видео
- **Симптомы**: Ошибки OpenCV при обработке видео
- **Обработка**: Логирование ошибки, пропуск проблемного видео
- **Восстановление**: Следующее видео обрабатывается нормально

### Ошибки MongoDB операций
- **Симптомы**: Не удаётся записать/прочитать данные из MongoDB
- **Обработка**: Логирование ошибки, продолжение работы
- **Восстановление**: Повторная обработка при следующей итерации

### Ошибки кодирования/декодирования кадров
- **Симптомы**: Ошибки base64 кодирования или JPEG декодирования
- **Обработка**: Логирование ошибки, пропуск проблемного кадра
- **Восстановление**: Следующий кадр обрабатывается нормально

### Стратегия обработки ошибок

1. **Логирование**: Все ошибки логируются с детальной информацией
2. **Продолжение работы**: Сервис продолжает работать при временных сбоях
3. **Повторная обработка**: Kafka сообщения не коммитятся при ошибках
4. **Автоматическое восстановление**: При восстановлении сервисов обработка продолжается автоматически

## Логирование

Детальное логирование всех операций:
- Подключение к Kafka
- Скачивание файлов из MinIO
- Извлечение и отправка кадров
- Получение и обработка результатов
- Ошибки и исключения

## Важные замечания

1. **Надёжность**: Отключён авто-коммит Kafka для гарантированной обработки
2. **Масштабируемость**: Можно запускать несколько экземпляров для балансировки нагрузки
3. **Отказоустойчивость**: Повторные попытки подключения к внешним сервисам
4. **Эффективность**: Оптимизированная обработка кадров с использованием асинхронности
