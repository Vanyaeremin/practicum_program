# Microservices Video Detection System

<img width="1576" height="784" alt="image" src="https://github.com/user-attachments/assets/ac43a9a3-7a96-4a60-8a2a-80fd24f003a9" />

## Обзор проекта

**Microservices Video Detection System** — распределённая система детекции объектов на видео с использованием YOLO модели. Система построена по микросервисной архитектуре с использованием паттерна Transactional Outbox для обеспечения надёжности и отказоустойчивости.

### Основные возможности

- **Детекция объектов** на видео с использованием YOLO модели
- **Обработка по кадрам** для оптимизации производительности
- **Масштабируемость** горизонтального масштабирования сервисов
- **Надёжность** с использованием паттерна Transactional Outbox
- **Асинхронная обработка** для высокой производительности
- **REST API** для загрузки видео и получения результатов

## Архитектура системы


### Поток данных

1. **API Checkout** → Приём видео → MinIO + MongoDB main_db.outbox
2. **Outbox Worker** → MongoDB main_db.outbox → Kafka (outbox_events)
3. **Runner** → Kafka → Извлечение кадров → Kafka (inference_tasks)
4. **Inference** → Kafka → YOLO детекция → Kafka (inference_results)
5. **Runner** → Kafka → Сбор результатов → MongoDB runner_db.runner_predictions
6. **Outbox S3 Worker** → MongoDB runner_db → MinIO (detection/*.json)
7. **API Checkout** → MinIO → Отдача результатов клиенту

## API Эндпоинты

### API Checkout Service (Порт: 8080)

#### Health Check
- `GET /api/`, `GET /api` — Health check
- `GET /api/health` — Health check

#### Основные операции
- `POST /api/upload_video` — Загрузка видео для обработки
- `POST /api/request` — Создание outbox события
- `GET /api/status/all` — Получение статусов всех операций
- `GET /api/result_video/{event_id}` — Получение результата обработки видео
- `DELETE /api/cleanup/all` — Полная очистка системы

### Runner Service (Порт: 8002)
- `GET /health` — Health check сервиса

### Inference Service (Порт: 8003)
- `GET /data`, `GET /api/data` — Базовый эндпоинт сервиса
- `GET /health` — Health check (если определён)

## Подробное описание пути видео

### Этап 1: Загрузка видео
```
Клиент → POST /upload_video → API Checkout Service
```

1. **Приём файла**: Клиент отправляет видеофайл через POST запрос
2. **Генерация ID**: Создаётся уникальный `video_id` (UUID4)
3. **Сохранение в MinIO**: Видео сохраняется по пути `input/{video_id}.mp4`
4. **Создание события**: В MongoDB outbox создаётся запись:
   ```json
   {
     "_id": "video-uuid",
     "status": "new",
     "payload": {
       "bucket": "videos",
       "object": "input/video-uuid.mp4",
       "media_type": "video"
     },
     "created_at": "2024-01-01T12:00:00Z"
   }
   ```
5. **Статус**: `new` — Новое событие создано

### Этап 2: Отправка в очередь обработки
```
MongoDB outbox → Outbox Worker → Kafka (outbox_events)
```

1. **Опрос**: Outbox Worker находит события со статусом `new`
2. **Публикация**: Событие отправляется в Kafka тему `outbox_events`
3. **Обновление статуса**: `new` → `sent`
4. **Статус**: `sent` — Событие отправлено в очередь обработки

### Этап 3: Предобработка видео
```
Kafka (outbox_events) → Runner Service → MinIO → Kafka (inference_tasks)
```

1. **Получение задачи**: Runner Service получает сообщение из Kafka
2. **Скачивание видео**: Файл скачивается из MinIO (`input/{video_id}.mp4`)
3. **Извлечение кадров**: Каждый 5-й кадр извлекается и предобрабатывается:
   - Gaussian blur (ksize=3)
   - Корректировка контраста (alpha=1.3, beta=5)
4. **Инициализация трекинга**: Создаётся запись в `runner_db.runner_predictions`:
   ```json
   {
     "_id": "video-uuid",
     "total_frames": 30,
     "received_frames": 0,
     "frame_results": {},
     "status": "processing"
   }
   ```
5. **Отправка кадров**: Каждый кадр отправляется в Kafka тему `inference_tasks`
6. **Инициализация трекинга**: Создаётся запись в `runner_db.runner_predictions` со статусом `processing`
7. **Статус**: `processing` (runner_db) — Видео обрабатывается в runner (внутренний статус, не видимый через API)

### Этап 4: Детекция объектов
```
Kafka (inference_tasks) → Inference Service → YOLO → Kafka (inference_results)
```

1. **Получение кадра**: Inference Service получает кадр (base64 JPEG)
2. **Декодирование**: Base64 → JPEG → numpy array
3. **Детекция YOLO**:
   - Модель: `best.pt`
   - Порог уверенности: 0.25
   - Порог IoU: 0.45
4. **Форматирование результата**:
   ```json
   {
     "frame": 0,
     "objects": [
       {"class": "cat", "box": [100, 200, 300, 400], "conf": 0.87}
     ]
   }
   ```
5. **Отправка результата**: В Kafka тему `inference_results`
6. **Статус**: Кадры обрабатываются в inference (статус не обновляется в outbox, соответствует принципам микросервисов)

### Этап 5: Сбор результатов
```
Kafka (inference_results) → Runner Service → MongoDB runner_db.runner_predictions
```

1. **Получение результатов**: Runner Service получает результаты по кадрам
2. **Сохранение кадров**: Результаты сохраняются в `runner_db.runner_predictions`:
   ```json
   {
     "frame_results": {
       "0": {"frame": 0, "objects": [...]},
       "5": {"frame": 5, "objects": [...]}
     },
     "received_frames": 30
   }
   ```
3. **Проверка завершения**: Когда `received_frames >= total_frames`
4. **Объединение результатов**: Сортировка по frame_id и объединение:
   ```json
   {
     "prediction": [
       {"frame": 0, "objects": [...]},
       {"frame": 5, "objects": [...]}
     ],
     "status": "predictable"
   }
   ```
5. **Обновление статуса**: 
   - В runner_db: `processing` → `predictable`
   - Outbox НЕ обновляется (соответствует принципам микросервисов)
6. **Статус**: `predictable` (runner_db) — Детекция завершена, результат готов к сохранению в S3

### Этап 6: Сохранение в S3
```
MongoDB runner_db → Outbox S3 Worker → MinIO (detection/)
```

1. **Опрос**: Outbox S3 Worker находит записи напрямую в `runner_db.runner_predictions` со статусом `predictable`
2. **Чтение данных**: Из записи в runner_db берутся `event_id`, `bucket` и `prediction` (финальный JSON)
3. **Сохранение в MinIO**: JSON сохраняется по пути `detection/{video_id}.json`
4. **Обновление runner_db** (НЕ outbox - соответствует принципам микросервисов):
   ```json
   {
     "status": "ready",
     "ready_at": "2024-01-01T12:05:00Z",
     "output_s3": {
       "bucket": "videos",
       "object": "detection/video-uuid.json"
     }
   }
   ```
5. **Сохранение в runner_db**: Данные остаются в `runner_db.runner_predictions` (не удаляются)
6. **Статус**: `ready` (runner_db) — Результат сохранён в S3 и готов к забору

### Этап 7: Получение результата
```
Клиент → GET /result_video/{event_id} → API Checkout → MinIO → Клиент
```

1. **Запрос результата**: Клиент запрашивает результат по `video_id`
2. **Проверка наличия JSON**: API проверяет наличие файла `detection/{video_id}.json` в S3
3. **Если файл есть**: JSON загружается из S3 и возвращается клиенту (статус "ready")
4. **Если файла нет**: Возвращается статус из outbox ("new" или "sent")

## Система статусов

### Упрощённая архитектура статусов

API использует упрощённый подход для определения статусов - проверяет наличие JSON файла в S3:

#### Статусы, видимые клиенту через API
1. **`new`** — Новое событие создано, ожидает отправки в очередь (из outbox)
2. **`sent`** — Событие отправлено в очередь обработки (из outbox)
3. **`ready`** — Результат сохранён в S3 и готов к забору (определяется наличием JSON файла в S3)

#### Логика определения статуса
- **API НЕ обращается к runner_db**
- **API проверяет наличие JSON файла** в S3 по пути `detection/{event_id}.json`
- **Если файл есть в S3** → статус `"ready"` (готов к забору)
- **Если файла нет в S3** → статус из outbox (`"new"` или `"sent"`)

#### Внутренние статусы (не видимые через API)
Внутри системы используются дополнительные статусы в runner_db:
- **`processing`** — Видео обрабатывается: кадры извлекаются и отправляются в inference
- **`predictable`** — Детекция завершена, все кадры обработаны, результат готов к сохранению в S3
- **`ready`** — Результат сохранён в S3 (используется только внутри runner_db)

### Полный жизненный цикл
1. **`new`** (outbox) — Новое событие создано
2. **`sent`** (outbox) — Событие отправлено в Kafka
3. **`processing`** (runner_db, внутренний) — Видео обрабатывается
4. **`predictable`** (runner_db, внутренний) — Результат готов к сохранению в S3
5. **`ready`** (определяется наличием JSON в S3) — Результат готов к забору

### Преимущества упрощённой архитектуры
- **Меньше зависимостей**: API не зависит от runner_db
- **Простота**: Один источник истины для готовности результата - наличие файла в S3
- **Надёжность**: S3 является источником истины для готовности результата
- **Масштабируемость**: API может работать независимо от runner_db

## Микросервисная архитектура баз данных

### Разделение баз данных
Система использует две отдельные MongoDB базы данных для поддержания микросервисной архитектуры:

#### main_db
- **outbox** — события для обработки и их статусы

#### runner_db  
- **runner_predictions** — результаты обработки кадров от сервиса runner
- Данные остаются в БД после сохранения в S3 (для истории и отладки)

### Преимущества разделения
1. **Независимость сервисов** — каждый сервис имеет свою БД
2. **Изоляция данных** — проблемы в одном сервисе не влияют на другие
3. **Масштабируемость** — БД можно масштабировать независимо
4. **История данных** — результаты сохраняются в runner_db для аналитики

## Асинхронность и борьба с GIL

### Проблема GIL (Global Interpreter Lock) — подробное объяснение

**GIL (Global Interpreter Lock)** — это механизм в CPython (стандартной реализации Python), который предотвращает одновременное выполнение байт-кода несколькими потоками в одном процессе. GIL — это мьютекс (mutex), который блокирует выполнение Python-кода в других потоках, пока один поток выполняет байт-код.

#### Почему GIL существует?
- **Упрощение управления памятью**: GIL защищает внутренние структуры данных CPython от гонок данных (race conditions)
- **Интеграция с C-расширениями**: Многие библиотеки (NumPy, OpenCV) написаны на C и не потокобезопасны
- **Исторические причины**: CPython был создан до эры многоядерных процессоров

#### Проблемы, которые создаёт GIL:
1. **Нет истинного параллелизма для CPU-bound задач**: Даже на многоядерном CPU, Python-код выполняется только в одном потоке одновременно
2. **Блокировка event loop**: Синхронные CPU-интенсивные операции блокируют весь event loop в асинхронных приложениях
3. **Неэффективное использование ресурсов**: Многоядерные CPU используются не полностью для Python-кода

### Решения в проекте — детальное описание

#### 1. Асинхронный фреймворк FastAPI и избегание блокировки Event Loop

**Проблема**: FastAPI использует asyncio event loop для обработки запросов. Если выполнить синхронную блокирующую операцию (например, запрос к MongoDB) напрямую в async функции, весь event loop заблокируется, и сервис не сможет обрабатывать другие запросы.

**Решение**: Использование `run_in_executor` для вынесения блокирующих операций в отдельный поток/процесс.

```python
# API Checkout Service — пример из app.py
@app.get("/api/status/all")
async def get_all_statuses():
    loop = asyncio.get_event_loop()
    
    def get_events():
        # Эта функция выполняется в отдельном потоке
        # Event loop НЕ блокируется во время выполнения
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        events = list(outbox.find({}, {"_id": 1, "status": 1, "created_at": 1}))
        client.close()
        return events
    
    # run_in_executor(None, ...) использует ThreadPoolExecutor по умолчанию
    # Это позволяет event loop обрабатывать другие запросы во время выполнения get_events()
    events = await loop.run_in_executor(None, get_events)
    return {"operations": result, "total": len(result)}
```

**Как это работает**:
- `run_in_executor(None, func)` использует `ThreadPoolExecutor` по умолчанию
- Функция `get_events()` выполняется в отдельном потоке
- Event loop продолжает обрабатывать другие запросы, пока `get_events()` выполняется
- Когда `get_events()` завершается, результат возвращается в async функцию

**Преимущества**:
- Event loop не блокируется
- Сервис остаётся отзывчивым для других запросов
- Можно обрабатывать множество запросов одновременно

#### 2. run_in_executor для I/O-блокирующих операций (MongoDB, MinIO)

**Проблема**: Синхронные клиенты MongoDB (pymongo) и MinIO блокируют event loop при выполнении операций.

**Решение в Runner Service**:
```python
# runner/app.py — скачивание файла из MinIO
loop = asyncio.get_running_loop()
# download_image — синхронная функция из s3_utils.py
# Выполняется в отдельном потоке, event loop не блокируется
file_bytes = await loop.run_in_executor(None, download_image, bucket, object_name)
```

**Решение в Inference Service**:
```python
# inference/kafka_consumer.py — обработка кадра
# ВАЖНО: Inference Service НЕ обновляет outbox - это соответствует принципам микросервисов
# Статусы обработки отслеживаются в runner_db через runner service
loop = asyncio.get_running_loop()
frame_result = await loop.run_in_executor(
    None,
    get_yolo_predictions_for_frame_base64,
    frame_data, frame_id
)
```

**Решение в API Checkout Service**:
```python
# api_checkout/app.py — чтение JSON из MinIO
def read_from_minio():
    minio_cli = get_minio_client()
    response = minio_cli.get_object(detection_bucket, detection_key)
    json_data = response.read().decode("utf-8")
    response.close()
    response.release_conn()
    return json_data

loop = asyncio.get_running_loop()
json_data = await loop.run_in_executor(None, read_from_minio)
```

**Как это работает**:
- `run_in_executor(None, func)` создаёт задачу в ThreadPoolExecutor
- Синхронная функция выполняется в отдельном потоке
- Event loop продолжает работать и обрабатывать другие задачи
- Когда функция завершается, результат возвращается через await

#### 3. ProcessPoolExecutor для CPU-интенсивных задач (обход GIL)

**Проблема**: CPU-интенсивные операции (обработка изображений, извлечение кадров из видео) блокируют GIL и event loop, даже если выполняются через `run_in_executor(None, ...)` в потоке.

**Решение**: Использование `ProcessPoolExecutor` вместо `ThreadPoolExecutor`. Процессы имеют отдельные GIL, что обеспечивает истинный параллелизм.

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
    # Функция preprocess_image выполняется в отдельном процессе
    return await loop.run_in_executor(
        executor,  # ProcessPoolExecutor вместо None
        preprocess_image,  # CPU-интенсивная функция из image_cpu.py
        img_bytes
    )
```

**Как это работает**:
- `ProcessPoolExecutor` создаёт отдельные процессы Python
- Каждый процесс имеет свой собственный GIL
- Несколько процессов могут выполнять CPU-интенсивный код одновременно на разных ядрах CPU
- Event loop не блокируется, так как выполнение происходит в отдельном процессе

**Пример использования в Runner Service**:
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

**Преимущества ProcessPoolExecutor**:
- **Истинный параллелизм**: Используются все ядра CPU
- **Обход GIL**: Каждый процесс имеет свой GIL
- **Не блокирует event loop**: Выполнение в отдельном процессе
- **Масштабируемость**: Можно обрабатывать несколько видео одновременно

**Недостатки ProcessPoolExecutor**:
- **Overhead**: Создание процессов дороже, чем создание потоков
- **Сериализация данных**: Данные должны быть сериализуемы (pickle) для передачи между процессами
- **Память**: Каждый процесс имеет свою копию данных

#### 4. Асинхронные клиенты Kafka (aiokafka) — избегание блокировки Event Loop

**Проблема**: Синхронный клиент Kafka (kafka-python) блокирует event loop при отправке/получении сообщений.

**Решение**: Использование `aiokafka` — полностью асинхронного клиента Kafka.

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
    # Обработка сообщения
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

**Как это работает**:
- `aiokafka` использует asyncio для неблокирующих операций
- Все операции (подключение, отправка, получение) являются async/await
- Event loop не блокируется во время ожидания ответа от Kafka
- Можно обрабатывать множество сообщений одновременно

**Преимущества aiokafka**:
- **Не блокирует event loop**: Все операции асинхронные
- **Высокая производительность**: Может обрабатывать тысячи сообщений в секунду
- **Масштабируемость**: Можно обрабатывать множество топиков и партиций одновременно

### Сравнение подходов

| Подход | Когда использовать | GIL | Event Loop | Параллелизм |
|--------|-------------------|-----|------------|-------------|
| `run_in_executor(None, ...)` | I/O операции (MongoDB, MinIO) | Блокирует в потоке | Не блокирует | Нет (один поток) |
| `run_in_executor(ProcessPoolExecutor, ...)` | CPU-интенсивные задачи | Обходит (отдельные процессы) | Не блокирует | Да (много процессов) |
| `aiokafka` | Работа с Kafka | Не применимо | Не блокирует | Да (асинхронный I/O) |
| Синхронный код напрямую | ❌ Никогда в async функциях | Блокирует | Блокирует | Нет |

### Преимущества асинхронного подхода

1. **Высокая производительность** — одновременная обработка множества запросов без блокировки
2. **Эффективное использование ресурсов** — нет блокировок на I/O операциях, использование всех ядер CPU для CPU-bound задач
3. **Масштабируемость** — горизонтальное масштабирование без изменений кода
4. **Отзывчивость** — сервисы остаются доступны во время обработки тяжёлых задач
5. **Обход GIL** — ProcessPoolExecutor обеспечивает истинный параллелизм для CPU-интенсивных задач

## Паттерн Transactional Outbox — подробное объяснение

### Что такое Transactional Outbox

**Transactional Outbox** — это паттерн проектирования для обеспечения надёжной доставки событий в распределённых системах. Паттерн решает критическую проблему **двойной записи (double-write)** в распределённых системах, когда нужно атомарно обновить базу данных и отправить событие в message queue.

### Проблема без Outbox (Double-Write Problem)

В распределённых системах часто возникает необходимость:
1. Сохранить данные в базу данных
2. Отправить событие в message queue (Kafka, RabbitMQ и т.д.)

**Проблема**: Эти две операции не могут быть выполнены атомарно в разных системах.

```
Клиент → API Service → База данных (транзакция)
                          ↓
                       Message Queue (отправка события)
```

**Сценарии сбоя**:
1. **Сервис упал после записи в БД, но до отправки в queue**:
   - Данные сохранены в БД ✅
   - Событие не отправлено в queue ❌
   - Результат: Данные есть, но обработка не началась

2. **Сервис упал после отправки в queue, но до коммита транзакции БД**:
   - Событие отправлено в queue ✅
   - Транзакция БД не закоммичена ❌
   - Результат: Обработка началась, но данных нет в БД

3. **Сеть недоступна при отправке в queue**:
   - Данные сохранены в БД ✅
   - Ошибка отправки в queue ❌
   - Результат: Данные есть, но событие потеряно

### Решение с Transactional Outbox

Паттерн решает проблему, разделяя операции на два этапа:

```
Клиент → API Service → База данных (outbox таблица) [АТОМАРНАЯ ТРАНЗАКЦИЯ]
                          ↓
                    Outbox Worker → Message Queue (асинхронная отправка)
```

**Принцип работы**:
1. **Атомарная запись**: API Service записывает данные в основную таблицу И в outbox таблицу в одной транзакции
2. **Асинхронная отправка**: Отдельный worker (Outbox Worker) периодически опрашивает outbox и отправляет события в queue
3. **Гарантия доставки**: Событие остаётся в outbox до успешной отправки в queue
4. **Идемпотентность**: Повторная обработка безопасна (статус обновляется только после успешной отправки)

### Реализация в проекте

#### 1. Outbox Worker (MongoDB → Kafka)

**Назначение**: Обеспечивает надёжную доставку событий из MongoDB outbox в Kafka.

**Архитектура**:
```python
# outbox/outbox_worker.py
from kafka import KafkaProducer
from pymongo import MongoClient

# Подключение к MongoDB и Kafka
client = MongoClient("mongodb://admin:password@mongo:27017/")
db = client['main_db']
outbox = db['outbox']

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,  # Сериализация ключа
    retries=5,  # Повторные попытки при ошибках
    # Идемпотентность на уровне producer - предотвращает дубликаты сообщений
    enable_idempotence=True,  # Включает идемпотентность
    max_in_flight_requests_per_connection=1,  # Требуется для идемпотентности
    acks='all',  # Ждём подтверждения от всех реплик
)

# Основной цикл обработки
while True:
    # Ищем события со статусом "new" (созданы в api_checkout)
    events = list(outbox.find({"status": "new"}).limit(10))
    
    for event in events:
        # ИСПРАВЛЕНИЕ: Проверка статуса перед обработкой (защита от race condition)
        current = outbox.find_one({"_id": event["_id"]})
        if not current or current.get("status") != "new":
            continue  # Уже обработано другим воркером
        
        try:
            # Формируем сообщение для Kafka
            message = {
                "_id": str(event["_id"]),
                "payload": event["payload"],  # bucket, object, media_type
                "created_at": str(event.get("created_at")),
            }
            
            # ИСПРАВЛЕНИЕ: Отправка с message key для идемпотентности
            # Идемпотентный producer гарантирует, что даже при повторной отправке
            # (если update_one упадёт) сообщение не будет дублировано в Kafka
            event_id_str = str(event["_id"])
            future = producer.send(
                "outbox_events",
                value=message,
                key=event_id_str  # Используем event_id как ключ для партиционирования
            )
            future.get(timeout=10)  # Ждём подтверждения до 10 секунд
            
            # ИСПРАВЛЕНИЕ: Условное обновление статуса (идемпотентность)
            result = outbox.update_one(
                {"_id": event["_id"], "status": "new"},  # Условие для идемпотентности
                {"$set": {"status": "sent"}}
            )
            
            if result.modified_count == 0:
                print(f"Event {event['_id']} was already processed")
            else:
                print(f"Sent to Kafka: {message}")
            
        except Exception as e:
            # При ошибке событие остаётся со статусом "new"
            # Будет обработано при следующей итерации
            print(f"Failed to send to Kafka: {e}")
    
    # Проверяем новые события каждую секунду
    time.sleep(1)
```

**Отказоустойчивость**:
- **Повторные попытки**: Kafka producer имеет `retries=5` для автоматических повторов
- **Сохранение состояния**: Событие остаётся в outbox со статусом "new" при ошибке
- **Автоматическое восстановление**: При следующей итерации событие будет обработано снова
- **Идемпотентность**: Повторная отправка безопасна (Kafka гарантирует доставку)

**Проверка доступности Kafka**:
```python
# Ожидание готовности Kafka при старте
while True:
    try:
        producer = KafkaProducer(...)
        print("Connected to Kafka!")
        break
    except Exception as e:
        print("Kafka not ready yet, retrying...", e)
        time.sleep(5)  # Повторная попытка через 5 секунд
```

#### 2. Outbox S3 Worker (MongoDB → MinIO)

**Назначение**: Обеспечивает надёжное сохранение результатов детекции из MongoDB в MinIO (S3).

**Архитектура**:
```python
# outbox_s3/outbox_s3_worker.py
from pymongo import MongoClient
from minio import Minio

# Подключение к основной MongoDB (main_db) и runner_db
client = MongoClient(MONGO_URI)
db = client['main_db']
outbox = db['outbox']

runner_client = MongoClient(RUNNER_MONGO_URI)
runner_db = runner_client['runner_db']
runner_predictions = runner_db['runner_predictions']

# Подключение к MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# Основной цикл обработки
while True:
    try:
        # Ищем записи напрямую в runner_db со статусом "predictable" (финальный JSON готов)
        ready_predictions = list(runner_predictions.find({"status": "predictable"}).limit(10))
        
        for runner_pred in ready_predictions:
            event_id = runner_pred["_id"]
            bucket = runner_pred.get("bucket", "videos")
            prediction = runner_pred.get("prediction")
            detection_key = f"detection/{event_id}.json"
            
            if not prediction:
                print(f"No prediction found for {event_id}, skipping")
                continue
            
            # ИСПРАВЛЕНИЕ: Проверка статуса перед обработкой (защита от race condition)
            current = runner_predictions.find_one({"_id": event_id})
            if not current or current.get("status") != "predictable":
                continue  # Уже обработано другим воркером
            
            try:
                # ИСПРАВЛЕНИЕ: Проверка существования файла в S3 (идемпотентность)
                file_exists = False
                try:
                    minio_client.stat_object(bucket, detection_key)
                    file_exists = True
                except Exception:
                    file_exists = False
                
                # Сохраняем prediction JSON в MinIO только если файла нет
                if not file_exists:
                    json_bytes = json.dumps(prediction, ensure_ascii=False, indent=2).encode("utf-8")
                    
                    minio_client.put_object(
                        bucket,
                        detection_key,
                        data=io.BytesIO(json_bytes),
                        length=len(json_bytes),
                        content_type="application/json"
                    )
                    print(f"Saved prediction to S3: {bucket}/{detection_key}")
                else:
                    print(f"File already exists in S3: {bucket}/{detection_key} (idempotency)")
                
                # ИСПРАВЛЕНИЕ: Условное обновление статуса (идемпотентность)
                ready_at = datetime.utcnow()
                result = runner_predictions.update_one(
                    {"_id": event_id, "status": "predictable"},  # Условие для идемпотентности
                    {
                        "$set": {
                            "status": "ready",
                            "ready_at": ready_at,
                            "output_s3": {
                                "bucket": bucket,
                                "object": detection_key
                            }
                        }
                    }
                )
                
                if result.modified_count == 0:
                    print(f"Event {event_id} was already processed")
                else:
                    print(f"Status updated to 'ready' for {event_id}")
                
                # Данные остаются в runner_db (не удаляются) для истории
                
            except Exception as e:
                # При ошибке запись остаётся со статусом "predictable"
                # Будет обработано при следующей итерации
                print(f"Failed to save to S3 for {event_id}: {e}")
                
    except Exception as e:
        print(f"Worker error: {e}")
    
    # Проверяем каждую секунду
    time.sleep(1)
```

**Отказоустойчивость**:
- **Проверка доступности MinIO**: Ожидание готовности при старте
- **Сохранение состояния**: Запись остаётся со статусом "predictable" в runner_db при ошибке
- **Автоматическое восстановление**: При следующей итерации запись будет обработана снова
- **Микросервисная архитектура**: Данные читаются из runner_db, что обеспечивает изоляцию сервисов
- **Не обновляет outbox**: Outbox S3 Worker НЕ обновляет outbox - только runner_db

**Проверка доступности MinIO**:
```python
# Ожидание готовности MinIO при старте
minio_client = None
while True:
    try:
        minio_client = Minio(...)
        print("[outbox_s3] Connected to MinIO!")
        break
    except Exception as e:
        print(f"[outbox_s3] MinIO not ready yet, retrying... {e}")
        time.sleep(5)  # Повторная попытка через 5 секунд
```

### Преимущества паттерна Transactional Outbox

1. **Надёжность** — события не теряются при сбоях сервисов или сети
2. **Согласованность** — транзакционная целостность данных (запись в БД и outbox атомарна)
3. **Отказоустойчивость** — автоматическое восстановление при временных сбоях
4. **Масштабируемость** — можно запускать несколько worker'ов для параллельной обработки
5. **Мониторинг** — все события отслеживаются в БД, можно видеть статусы обработки
6. **Идемпотентность** — повторная обработка безопасна (статус обновляется только после успеха)
7. **Разделение ответственности** — основной сервис не зависит от доступности message queue

### Жизненный цикл события в Outbox

**Outbox (main_db.outbox)** - используется только для outbox паттерна:
1. **Создание**: API Service создаёт событие в outbox со статусом `"new"` (атомарно с основной записью)
2. **Отправка**: Outbox Worker находит событие со статусом `"new"` и отправляет в Kafka
3. **Подтверждение**: После успешной отправки статус обновляется на `"sent"` (после этого outbox больше не обновляется)

**Runner DB (runner_db.runner_predictions)** - хранит статусы обработки (внутренние):
4. **Обработка**: Runner Service получает событие из Kafka и обрабатывает, устанавливает статус `"processing"` в runner_db
5. **Готовность**: Runner устанавливает статус `"predictable"` в runner_db (готов к сохранению в S3)
6. **Сохранение**: Outbox S3 Worker находит запись в runner_db со статусом `"predictable"` и сохраняет результат в MinIO по пути `detection/{event_id}.json`
7. **Завершение**: После успешного сохранения статус обновляется на `"ready"` в runner_db

### Исправления проблем атомарности и идемпотентности (2024)

**Обнаруженные проблемы:**

В первоначальной реализации обоих воркеров (outbox_worker и outbox_s3_worker) были обнаружены проблемы с атомарностью операций и идемпотентностью:

1. **Отсутствие идемпотентности**: При повторной обработке событий/записей могли создаваться дубликаты сообщений в Kafka или файлов в S3.

2. **Race condition**: При параллельной работе нескольких экземпляров воркеров одно и то же событие/запись могло быть обработано одновременно.

3. **Проблема атомарности между внешними системами и MongoDB**: 
   - В outbox_worker: если `producer.send()` успешен, но `update_one()` упал, событие могло быть отправлено повторно.
   - В outbox_s3_worker: если `put_object()` успешен, но `update_one()` упал, файл уже был в S3, но статус оставался "predictable", что могло привести к перезаписи файла.

4. **Отсутствие подтверждения отправки**: В outbox_worker не ожидалось подтверждение успешной отправки в Kafka перед обновлением статуса.

**Внесённые исправления:**

#### Outbox Worker (MongoDB → Kafka):

1. **Проверка статуса перед обработкой**: Перед обработкой события проверяется, что его статус всё ещё "new". Если статус изменился (обработан другим воркером), событие пропускается.

2. **Ожидание подтверждения отправки**: Добавлен `future.get(timeout=10)` для ожидания подтверждения успешной отправки сообщения в Kafka перед обновлением статуса.

3. **Условное обновление статуса**: Обновление статуса выполняется только если текущий статус равен "new" (`{"_id": event["_id"], "status": "new"}`). Это обеспечивает идемпотентность и защиту от race condition.

4. **Проверка результата обновления**: После обновления проверяется `result.modified_count` для определения, было ли обновление успешным.

5. **Идемпотентный Kafka Producer**: Добавлен идемпотентный producer с параметрами:
   - `enable_idempotence=True` — включает идемпотентность на уровне producer
   - `max_in_flight_requests_per_connection=1` — требуется для идемпотентности (Kafka requirement)
   - `acks='all'` — ждём подтверждения от всех реплик (требуется для идемпотентности)
   - `key=event_id` — используем event_id как ключ сообщения для партиционирования
   
   **Зачем это нужно:**
   - Kafka присваивает producer ID (PID) и sequence number каждому сообщению
   - При повторной отправке (если `update_one()` упадёт после успешной отправки) брокер распознаёт дубликат по PID + sequence number и отклоняет его
   - Это **полностью решает проблему атомарности между Kafka и MongoDB** — даже если `update_one()` упадёт, сообщение не будет дублировано в Kafka
   - Message key гарантирует, что все сообщения с одним `event_id` попадут в одну partition, обеспечивая порядок обработки

#### Outbox S3 Worker (MongoDB → MinIO):

1. **Проверка статуса перед обработкой**: Перед обработкой записи проверяется, что её статус всё ещё "predictable". Если статус изменился (обработан другим воркером), запись пропускается.

2. **Проверка существования файла в S3**: Перед сохранением файла проверяется, не существует ли уже файл в S3 через `minio_client.stat_object()`. Если файл существует, сохранение пропускается, что обеспечивает идемпотентность.

3. **Условное обновление статуса**: Обновление статуса выполняется только если текущий статус равен "predictable" (`{"_id": event_id, "status": "predictable"}`). Это обеспечивает идемпотентность и защиту от race condition.

4. **Проверка результата обновления**: После обновления проверяется `result.modified_count` для определения, было ли обновление успешным.

**Цель исправлений:**

- **Идемпотентность**: Гарантировать, что повторная обработка событий/записей не создаст дубликаты
- **Защита от race condition**: Предотвратить параллельную обработку одного события/записи несколькими воркерами
- **Надёжность**: Обеспечить, что статус обновляется только после успешной внешней операции (отправка в Kafka или сохранение в S3)
- **Восстановление после сбоев**: Если внешняя операция успешна, но обновление БД упало, при следующей итерации операция будет пропущена (файл уже существует, статус уже обновлён), а статус будет обновлён корректно
- **Масштабируемость**: Позволить безопасно запускать несколько экземпляров воркеров для параллельной обработки

**Важное замечание:**

Полной атомарности между внешними системами (Kafka, MinIO) и MongoDB достичь невозможно, так как это разные распределённые системы. Однако исправления обеспечивают идемпотентность операций и устойчивость к сбоям, что является практическим решением для распределённых систем.

**Особое внимание — идемпотентный Kafka Producer:**

Идемпотентный producer в outbox_worker **полностью решает проблему атомарности между Kafka и MongoDB**:
- Если `producer.send()` успешен, но `update_one()` упадёт, при следующей итерации Kafka отклонит дубликат по PID + sequence number
- Сообщение не будет дублировано в Kafka, даже если статус в MongoDB не обновился
- Это обеспечивает exactly-once семантику на уровне Kafka, что критично для надёжности системы

Подробнее об исправлениях см. в README файлах:
- `outbox/README.md` — раздел "Исправления и улучшения"
- `outbox_s3/README.md` — раздел "Исправления и улучшения"

**API для определения статуса** (упрощённый подход):
- API НЕ обращается к runner_db
- API проверяет наличие JSON файла в S3 (`detection/{event_id}.json`)
- Если файл есть → статус `"ready"` (готов к забору)
- Если файла нет → статус из outbox (`"new"` или `"sent"`)

**ВАЖНО**: 
- Runner и Outbox S3 Worker НЕ обновляют outbox - это соответствует принципам микросервисной архитектуры
- API определяет готовность результата по наличию файла в S3, а не по статусу в runner_db

### Обработка ошибок в Outbox

**Стратегия восстановления**:
- **Временные ошибки**: Событие остаётся в outbox, будет обработано при следующей итерации
- **Постоянные ошибки**: Событие остаётся в outbox для ручного вмешательства
- **Мониторинг**: Логирование всех ошибок для отслеживания проблем
- **Retry механизм**: Kafka producer имеет встроенные retry для сетевых ошибок

## Отказоустойчивость и проверка доступности сервисов

### Общие принципы отказоустойчивости

Система спроектирована с учётом отказоустойчивости на всех уровнях:

1. **Graceful degradation**: При недоступности одного сервиса остальные продолжают работать
2. **Retry механизмы**: Автоматические повторные попытки при временных сбоях
3. **Health checks**: Проверка доступности сервисов перед использованием
4. **Circuit breaker**: Избегание каскадных сбоев при недоступности зависимостей
5. **Idempotency**: Повторная обработка безопасна (идемпотентные операции)

### Проверка доступности MongoDB

#### Runner Service — проверка доступности БД

**Проблема**: Если MongoDB недоступна при старте или во время работы, сервис должен корректно обрабатывать ошибки.

**Решение**: Использование try-except блоков и повторные попытки подключения.

```python
# runner/app.py — пример обработки ошибок MongoDB
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
```

**Проверка доступности при операциях**:
```python
# runner/app.py — сохранение результатов в runner_db
def save_frame_and_check_complete():
    """Сохраняет результат кадра в runner_db с проверкой доступности"""
    try:
        # Используем отдельную БД runner_db для микросервисной архитектуры
        db = get_runner_db()
        runner_predictions = db['runner_predictions']
        
        # Операции с БД
        runner_predictions.update_one(...)
        
    except Exception as e:
        print(f"[runner] MongoDB error saving frame result: {e}", flush=True)
        # При ошибке результат не сохраняется, но обработка продолжается
        # Kafka сообщение не коммитится, будет обработано повторно
```

#### Inference Service — проверка доступности БД

```python
# inference/kafka_consumer.py — обновление статуса
def update_outbox(event_id, status):
    """Обновляет статус в MongoDB с обработкой ошибок"""
    try:
        client = MongoClient(MONGO_URI)
        db = client['main_db']
        outbox = db['outbox']
        outbox.update_one({"_id": event_id}, {"$set": {"status": status}})
        client.close()
    except Exception as e:
        print(f"[inference] MongoDB error: {e}", flush=True)
        # При ошибке статус не обновляется, но обработка продолжается
```

#### API Checkout Service — проверка доступности БД

```python
# api_checkout/app.py — чтение статусов
async def get_all_statuses():
    """Получает статусы с обработкой ошибок MongoDB"""
    try:
        loop = asyncio.get_event_loop()
        
        def get_events():
            try:
                client = MongoClient("mongodb://admin:password@mongo:27017/")
                db = client['main_db']
                outbox = db['outbox']
                events = list(outbox.find({}, {"_id": 1, "status": 1, "created_at": 1}))
                client.close()
                return events
            except Exception as e:
                print(f"[api_checkout] MongoDB error: {e}", flush=True)
                return []  # Возвращаем пустой список при ошибке
        
        events = await loop.run_in_executor(None, get_events)
        # Обработка результатов...
        
    except Exception as e:
        print(f"[api_checkout] Error getting statuses: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"Status retrieval error: {str(e)}")
```

### Проверка доступности MinIO/S3

#### Runner Service — проверка доступности S3

**Проблема**: Если MinIO недоступен, сервис не может скачать видео для обработки.

**Решение**: Проверка доступности перед началом обработки и повторные попытки.

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

#### API Checkout Service — проверка доступности MinIO

```python
# api_checkout/app.py — загрузка видео
@app.post("/api/upload_video")
async def upload_and_enqueue_video(file: UploadFile = File(...), bucket: str = "videos"):
    try:
        # Получение MinIO клиента с обработкой ошибок
        def minio_upload():
            try:
                client = get_minio_client()  # Ленивая инициализация
                
                if not client.bucket_exists(bucket):
                    client.make_bucket(bucket)
                
                client.put_object(bucket, filename, data=io.BytesIO(data), ...)
            except Exception as e:
                print(f"[upload_video] MinIO error: {e}", flush=True)
                raise  # Пробрасываем ошибку для обработки выше
        
        await loop.run_in_executor(None, minio_upload)
        
    except Exception as upload_error:
        print(f"[upload_video] Error uploading to MinIO: {upload_error}", flush=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"MinIO upload error: {str(upload_error)}"
        )
```

### Проверка доступности Kafka

#### Runner Service — проверка доступности Kafka

```python
# runner/app.py — подключение к Kafka с повторными попытками
async def consume():
    """Чтение событий из Kafka с проверкой доступности"""
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
            await consumer.start()  # Асинхронное подключение
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

#### Inference Service — проверка доступности Kafka

```python
# inference/kafka_consumer.py — подключение к Kafka
async def consume_inference():
    # Ожидаем готовности Kafka для consumer
    while True:
        try:
            consumer = AIOKafkaConsumer(...)
            await consumer.start()
            print("[inference] aiokafka consumer connected", flush=True)
            break
        except Exception as e:
            print(f"[inference] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
    
    # Ожидаем готовности Kafka для producer
    producer = None
    while producer is None:
        try:
            producer = AIOKafkaProducer(...)
            await producer.start()
            print("[inference] aiokafka producer connected", flush=True)
        except Exception as e:
            print(f"[inference] aiokafka producer not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)
```

### Стратегии обработки ошибок

#### 1. Retry с экспоненциальной задержкой

```python
# Пример retry механизма (можно добавить в будущем)
import time

def retry_with_backoff(func, max_retries=5, initial_delay=1):
    """Повторная попытка с экспоненциальной задержкой"""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise  # Последняя попытка, пробрасываем ошибку
            print(f"Attempt {attempt + 1} failed: {e}, retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2  # Экспоненциальная задержка
```

#### 2. Circuit Breaker (можно добавить в будущем)

```python
# Пример circuit breaker паттерна
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half_open
    
    def call(self, func):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "half_open"
            else:
                raise Exception("Circuit breaker is open")
        
        try:
            result = func()
            if self.state == "half_open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            raise
```

#### 3. Graceful Shutdown

```python
# Пример graceful shutdown для Kafka consumer
import signal

shutdown_event = asyncio.Event()

def signal_handler():
    """Обработчик сигнала завершения"""
    print("Shutdown signal received, finishing current tasks...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def consume():
    consumer = AIOKafkaConsumer(...)
    await consumer.start()
    
    try:
        async for msg in consumer:
            if shutdown_event.is_set():
                break  # Завершаем обработку при получении сигнала
            
            # Обработка сообщения...
            await consumer.commit()
    finally:
        await consumer.stop()  # Корректное закрытие
```

### Мониторинг и логирование

Все сервисы используют детальное логирование для отслеживания ошибок:

```python
# Пример логирования во всех сервисах
print(f"[service_name] Operation started: {details}", flush=True)
print(f"[service_name] Operation completed: {result}", flush=True)
print(f"[service_name] Error occurred: {error}", flush=True)
```

**Преимущества логирования**:
- **Отладка**: Легко найти причину ошибки
- **Мониторинг**: Можно отслеживать состояние системы
- **Анализ**: Можно анализировать паттерны ошибок

### Health Checks

Все сервисы предоставляют health check эндпоинты:

```python
# API Checkout Service
@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "api_checkout"}

# Runner Service
@app.get("/health")
def health():
    return {"status": "ok"}

# Inference Service
@app.get("/health")
def health():
    return {"status": "ok"}
```

**Использование health checks**:
- **Docker health checks**: Docker может проверять здоровье контейнеров
- **Load balancer**: Может исключать нездоровые инстансы из балансировки
- **Мониторинг**: Системы мониторинга могут отслеживать доступность сервисов

## Технологический стек

### Backend
- **Python 3.9+** — основной язык разработки
- **FastAPI** — асинхронный веб-фреймворк
- **aiokafka** — асинхронный клиент Kafka
- **pymongo** — клиент MongoDB
- **minio** — S3-совместимый клиент

### ML/AI
- **Ultralytics YOLO** — детекция объектов
- **OpenCV** — обработка изображений и видео
- **NumPy** — операции с массивами

### Инфраструктура
- **Kafka** — message queue для асинхронной обработки
- **MongoDB** — хранилище статусов и промежуточных результатов
  - **main_db** — основная БД для outbox событий
  - **runner_db** — отдельная БД для сервиса runner
- **MinIO** — S3-совместимое объектное хранилище
- **Docker** — контейнеризация сервисов

## Развертывание

### Docker Compose
```bash
docker-compose up -d
```

Сервисы:
- `api_checkout` — порт 8080
- `runner` — порт 8002
- `inference` — порт 8003
- `outbox_worker` — фоновый процесс
- `outbox_s3_worker` — фоновый процесс
- `kafka` — порт 9092
- `mongo` — порт 27017
- `minio` — порт 9000

### Переменные окружения
```bash
# Основные БД
MONGO_URI=mongodb://admin:password@mongo:27017/
RUNNER_MONGO_URI=mongodb://admin:password@mongo:27017/

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

## Примеры использования

### Загрузка и обработка видео
```bash
# 1. Загрузка видео
curl -X POST "http://localhost:8080/api/upload_video" \
  -F "file=@video.mp4" \
  -F "bucket=videos"

# Ответ: {"status": "ok", "event_id": "video-uuid"}

# 2. Проверка статуса
curl "http://localhost:8080/api/status/all"

# 3. Получение результата
curl "http://localhost:8080/api/result_video/video-uuid"
```

### Пример результата
```json
{
  "status": "ready",
  "event_id": "video-uuid",
  "prediction": [
    {
      "frame": 0,
      "objects": [
        {
          "class": "cat",
          "box": [100, 200, 300, 400],
          "conf": 0.87
        }
      ]
    }
  ],
  "processing_time_seconds": 45,
  "processing_time_formatted": "45s"
}
```

## Мониторинг и отладка

### Логирование
Все сервисы используют детальное логирование с `flush=True`:
- Статусы обработки
- Ошибки и исключения
- Время выполнения операций
- Подключения к внешним сервисам

### Статусы обработки
```bash
# Получить все статусы
curl "http://localhost:8080/api/status/all"

# Ответ
{
  "operations": [
    {
      "video_id": "video-uuid",
      "status_code": "ready",
      "status_description": "Результат сохранён в S3 и готов к забору",
      "processing_time_seconds": 45,
      "processing_time_formatted": "45s"
    }
  ],
  "total": 1
}
```

## Будущие улучшения

1. **Web UI** — интерфейс для загрузки и просмотра результатов
2. **Масштабирование** — автоматическое масштабирование сервисов
3. **Мониторинг** — Prometheus + Grafana для метрик
4. **Тестирование** — автоматические тесты для всех сервисов
5. **Документация** — OpenAPI/Swagger для API документации
6. **Безопасность** — аутентификация и авторизация
7. **Кэширование** — Redis для кэширования результатов
8. **Балансировка** — nginx для балансировки нагрузки

