# API Checkout Service

## Обзор

**API Checkout Service** — входная точка системы детекции объектов на видео. Сервис отвечает за приём видео от пользователей, сохранение в объектное хранилище и создание событий для асинхронной обработки.

## Основные функции

1. **Приём и загрузка видео** через REST API эндпоинт `/api/upload_video`
2. **Сохранение видео** в MinIO (S3-совместимое хранилище) по пути `input/{video_id}.mp4`
3. **Создание событий** в outbox для асинхронной обработки через паттерн Transactional Outbox
4. **Предоставление результатов** детекции через API эндпоинты
5. **Мониторинг статусов** обработки видео в реальном времени

## API Эндпоинты

### Health Check
- `GET /api/`, `GET /api` — Health check
- `GET /api/health` — Health check

### Основные операции
- `POST /api/upload_video` — Загрузка видео для обработки
- `POST /api/request` — Создание outbox события
- `GET /api/status/all` — Получение статусов всех операций
- `GET /api/result_video/{event_id}` — Получение результата обработки видео
- `DELETE /api/cleanup/all` — Полная очистка системы

## Архитектура и взаимодействие

### Процесс загрузки видео
1. **Получение видео**: Клиент отправляет видеофайл через POST запрос на `/api/upload_video`
2. **Генерация ID**: Создаётся уникальный `video_id` (UUID4)
3. **Сохранение в MinIO**: Видео сохраняется по пути `input/{video_id}.mp4`
4. **Создание события**: В MongoDB outbox создаётся запись со статусом `"new"`
5. **Асинхронная обработка**: Outbox worker забирает событие и отправляет в Kafka

### Статусы обработки (упрощённая архитектура)
- `new` — Новое событие создано, ожидает отправки в очередь (из outbox)
- `sent` — Событие отправлено в очередь обработки (из outbox)
- `ready` — Результат сохранён в S3 и готов к забору (определяется наличием JSON файла в S3)

**Внутренние статусы** (не видимые через API, используются только в runner_db):
- `processing` — Видео обрабатывается: кадры извлекаются и отправляются в inference
- `predictable` — Детекция завершена, все кадры обработаны, результат готов к сохранению в S3
- `ready` — Результат сохранён в S3 (используется только внутри runner_db)

## Технические детали

### Зависимости
- **FastAPI** — веб-фреймворк для REST API
- **MinIO** — S3-совместимое объектное хранилище
- **MongoDB** — хранилище outbox событий и статусов
- **Pydantic** — валидация данных

### Конфигурация
Переменные окружения:
- `MINIO_ENDPOINT` — адрес MinIO сервера (по умолчанию: `minio:9000`)
- `MINIO_ACCESS_KEY` — ключ доступа MinIO (по умолчанию: `minioadmin`)
- `MINIO_SECRET_KEY` — секретный ключ MinIO (по умолчанию: `minioadmin`)

### Асинхронность и избегание блокировки Event Loop

Сервис активно использует асинхронность для обеспечения высокой производительности и отзывчивости:

#### 1. Асинхронный фреймворк FastAPI

**Проблема**: FastAPI использует asyncio event loop для обработки запросов. Если выполнить синхронную блокирующую операцию (например, запрос к MongoDB или MinIO) напрямую в async функции, весь event loop заблокируется, и сервис не сможет обрабатывать другие запросы.

**Решение**: Использование `run_in_executor` для вынесения блокирующих операций в отдельный поток.

#### 2. run_in_executor для MongoDB операций

**Проблема**: Синхронный клиент MongoDB (pymongo) блокирует event loop при выполнении операций.

**Решение**: Использование `run_in_executor` для вынесения MongoDB операций в отдельный поток.

```python
# api_checkout/app.py — чтение статусов из MongoDB
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

```python
# api_checkout/app.py — создание outbox события
@app.post("/api/upload_video")
async def upload_and_enqueue_video(file: UploadFile = File(...), bucket: str = "videos"):
    loop = asyncio.get_event_loop()
    
    def mongo_insert():
        # Синхронная функция для вставки в MongoDB
        client = MongoClient("mongodb://admin:password@mongo:27017/")
        db = client['main_db']
        outbox = db['outbox']
        event = {
            "_id": video_id,
            "payload": {...},
            "created_at": datetime.utcnow(),
            "status": "new"
        }
        outbox.insert_one(event)
        client.close()
    
    # Выполняется в отдельном потоке, event loop не блокируется
    await loop.run_in_executor(None, mongo_insert)
```

**Как это работает**:
- `run_in_executor(None, func)` использует `ThreadPoolExecutor` по умолчанию
- Функция выполняется в отдельном потоке
- Event loop продолжает обрабатывать другие запросы
- Когда функция завершается, результат возвращается в async функцию

#### 3. run_in_executor для MinIO операций

**Проблема**: Синхронный клиент MinIO блокирует event loop при выполнении операций (загрузка, скачивание файлов).

**Решение**: Использование `run_in_executor` для вынесения MinIO операций в отдельный поток.

```python
# api_checkout/app.py — загрузка видео в MinIO
@app.post("/api/upload_video")
async def upload_and_enqueue_video(file: UploadFile = File(...), bucket: str = "videos"):
    loop = asyncio.get_event_loop()
    
    def minio_upload():
        # Синхронная функция для загрузки в MinIO
        client = get_minio_client()
        
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        client.put_object(
            bucket,
            filename,
            data=io.BytesIO(data),
            length=len(data),
            content_type="video/mp4"
        )
    
    # Выполняется в отдельном потоке, event loop не блокируется
    await loop.run_in_executor(None, minio_upload)
```

```python
# api_checkout/app.py — чтение JSON из MinIO
@app.get("/api/result_video/{event_id}")
async def get_result_video(event_id: str, bucket: str = "videos"):
    loop = asyncio.get_event_loop()
    
    def read_from_minio():
        # Синхронная функция для чтения из MinIO
        minio_cli = get_minio_client()
        response = minio_cli.get_object(detection_bucket, detection_key)
        json_data = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return json_data
    
    # Выполняется в отдельном потоке, event loop не блокируется
    json_data = await loop.run_in_executor(None, read_from_minio)
    prediction = json.loads(json_data)
    return {"status": status, "prediction": prediction, ...}
```

**Преимущества**:
- Event loop не блокируется
- Сервис остаётся отзывчивым для других запросов
- Можно обрабатывать множество запросов одновременно

### Борьба с GIL (Global Interpreter Lock)

**GIL (Global Interpreter Lock)** — это механизм в CPython, который предотвращает одновременное выполнение байт-кода несколькими потоками в одном процессе.

**Проблема в API Checkout Service**:
- Операции с MongoDB и MinIO — I/O операции, но выполняются через синхронные клиенты
- Если выполнять эти операции в основном потоке, event loop блокируется

**Решение**:
- **ThreadPoolExecutor** (через `run_in_executor(None, ...)`) для I/O операций:
  - MongoDB операции
  - MinIO операции
  - Event loop не блокируется

**Примечание**: API Checkout Service в основном выполняет I/O операции (MongoDB, MinIO), которые не требуют CPU-интенсивной обработки. Поэтому используется `ThreadPoolExecutor`, а не `ProcessPoolExecutor`.

**Пример использования**:
```python
# I/O операция (MongoDB) — используем ThreadPoolExecutor
events = await loop.run_in_executor(None, get_events)

# I/O операция (MinIO) — используем ThreadPoolExecutor
json_data = await loop.run_in_executor(None, read_from_minio)
```

## Примеры использования

### Загрузка видео
```bash
curl -X POST "http://localhost:8080/api/upload_video" \
  -F "file=@video.mp4" \
  -F "bucket=videos"
```

Ответ:
```json
{
  "status": "ok",
  "event_id": "123e4567-e89b-12d3-a456-426614174000"
}
```

### Получение статуса
```bash
curl "http://localhost:8080/api/status/all"
```

### Получение результата
```bash
curl "http://localhost:8080/api/result_video/123e4567-e89b-12d3-a456-426614174000"
```

## Отказоустойчивость и проверка доступности сервисов

### Проверка доступности MongoDB

Сервис обрабатывает ошибки MongoDB с логированием и возвратом соответствующих HTTP статусов:

```python
# api_checkout/app.py — чтение статусов с обработкой ошибок
@app.get("/api/status/all")
async def get_all_statuses():
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

**Преимущества**:
- **Обработка ошибок**: Все ошибки MongoDB логируются
- **Graceful degradation**: При ошибке возвращается пустой список или HTTP ошибка
- **Продолжение работы**: Сервис продолжает работать при временных сбоях БД

### Проверка доступности MinIO

Сервис обрабатывает ошибки MinIO с логированием и возвратом соответствующих HTTP статусов:

```python
# api_checkout/app.py — загрузка видео с обработкой ошибок
@app.post("/api/upload_video")
async def upload_and_enqueue_video(file: UploadFile = File(...), bucket: str = "videos"):
    try:
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

```python
# api_checkout/app.py — чтение результата с обработкой ошибок
@app.get("/api/result_video/{event_id}")
async def get_result_video(event_id: str, bucket: str = "videos"):
    try:
        def read_from_minio():
            try:
                minio_cli = get_minio_client()
                response = minio_cli.get_object(detection_bucket, detection_key)
                json_data = response.read().decode("utf-8")
                response.close()
                response.release_conn()
                return json_data
            except Exception as e:
                print(f"[get_result_video] MinIO error: {e}", flush=True)
                raise
        
        json_data = await loop.run_in_executor(None, read_from_minio)
        
    except Exception as e:
        print(f"[get_result_video] Error reading from MinIO: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"MinIO result read error: {str(e)}")
```

**Преимущества**:
- **Обработка ошибок**: Все ошибки MinIO логируются
- **HTTP статусы**: Возвращаются соответствующие HTTP статусы (500, 404 и т.д.)
- **Продолжение работы**: Сервис продолжает работать при временных сбоях MinIO

### Ленивая инициализация MinIO клиента

Сервис использует ленивую инициализацию MinIO клиента для оптимизации:

```python
# api_checkout/app.py — ленивая инициализация MinIO
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
```

**Преимущества**:
- **Оптимизация**: Клиент создаётся только при первом использовании
- **Обработка ошибок**: Ошибки инициализации логируются
- **Переиспользование**: Клиент переиспользуется для всех запросов

## Обработка ошибок

Сервис обрабатывает следующие типы ошибок с детальным логированием и соответствующими HTTP статусами:

### Ошибки подключения к MinIO
- **Симптомы**: Не удаётся подключиться к MinIO серверу
- **Обработка**: Логирование ошибки, возврат HTTP 500
- **Восстановление**: Повторная попытка при следующем запросе

### Ошибки сохранения в MongoDB
- **Симптомы**: Не удаётся сохранить событие в MongoDB
- **Обработка**: Логирование ошибки, возврат HTTP 500
- **Восстановление**: Повторная попытка при следующем запросе

### Ошибки формата файла
- **Симптомы**: Неверный формат загружаемого файла
- **Обработка**: Валидация через FastAPI, возврат HTTP 400
- **Восстановление**: Клиент должен отправить корректный файл

### Внутренние ошибки сервера
- **Симптомы**: Неожиданные исключения в коде
- **Обработка**: Логирование с traceback, возврат HTTP 500
- **Восстановление**: Анализ логов для исправления проблемы

### Стратегия обработки ошибок

1. **Логирование**: Все ошибки логируются с детальной информацией и traceback
2. **HTTP статусы**: Возвращаются соответствующие HTTP статусы (400, 404, 500 и т.д.)
3. **Описания на русском**: Все ошибки возвращаются с описанием на русском языке
4. **Graceful degradation**: При ошибках сервис возвращает понятные сообщения об ошибках
5. **Продолжение работы**: Сервис продолжает работать при временных сбоях зависимостей

## Логирование

Сервис использует `print()` с `flush=True` для логирования всех операций:
- Загрузка файлов в MinIO
- Создание событий в MongoDB
- Ошибки и исключения
- Статусы обработки

## Важные замечания

1. **Надёжность**: Используется паттерн Transactional Outbox для гарантированной доставки событий
2. **Масштабируемость**: Сервис может быть масштабирован горизонтально
3. **Отказоустойчивость**: Повторные попытки подключения к внешним сервисам
4. **Безопасность**: Валидация входных данных и обработка ошибок
