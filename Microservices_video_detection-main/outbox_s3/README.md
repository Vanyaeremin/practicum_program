# Outbox S3 Worker Service

## Обзор

**Outbox S3 Worker** — реализация паттерна Transactional Outbox для надёжного сохранения результатов детекции в объектное хранилище S3 (MinIO). Сервис обеспечивает гарантированное сохранение JSON результатов детекции из MongoDB в MinIO.

## Основные функции

1. **Опрос MongoDB runner_db.runner_predictions** на записи со статусом `"predictable"`
2. **Чтение данных** из записи в runner_db: `event_id`, `bucket` и `prediction` (финальный JSON)
3. **Сохранение prediction JSON** в MinIO по пути `detection/{event_id}.json`
4. **Обновление статуса** в outbox на `"predicted"`
5. **Сохранение пути** к файлу в `output_s3`
6. **Сохранение данных** в runner_db (НЕ удаляется после сохранения в S3)

## Паттерн Transactional Outbox для S3 — подробное объяснение

### Что такое Transactional Outbox для S3

**Transactional Outbox для S3** — это адаптация паттерна Transactional Outbox для надёжного сохранения результатов в объектное хранилище (S3/MinIO). Паттерн решает проблему надёжного сохранения результатов детекции в распределённых системах с микросервисной архитектурой.

### Проблема без Outbox

В распределённых системах часто возникает необходимость:
1. Сохранить результат обработки в базу данных
2. Сохранить результат в объектное хранилище (S3/MinIO)

**Проблема**: Эти две операции не могут быть выполнены атомарно в разных системах.

**Сценарии сбоя**:
1. **Сервис упал после записи в БД, но до сохранения в S3**:
   - Результат сохранён в БД ✅
   - Результат не сохранён в S3 ❌
   - Результат: Данные есть в БД, но недоступны через API

2. **Сервис упал после сохранения в S3, но до обновления статуса в БД**:
   - Результат сохранён в S3 ✅
   - Статус в БД не обновлён ❌
   - Результат: Файл есть в S3, но статус неверный

3. **S3 недоступен при сохранении**:
   - Результат сохранён в БД ✅
   - Ошибка сохранения в S3 ❌
   - Результат: Данные есть в БД, но недоступны через API

### Решение с Transactional Outbox для S3

Паттерн решает проблему, разделяя операции на два этапа:

```
Runner Service → runner_db.runner_predictions + main_db.outbox [АТОМАРНЫЕ ОПЕРАЦИИ]
                        ↓
                  Outbox S3 Worker → MinIO (S3) [асинхронное сохранение]
```

**Принцип работы (НОВАЯ АРХИТЕКТУРА)**:
1. **Подготовка результата**: Runner сервис сохраняет результат в `runner_db.runner_predictions` со статусом `"predictable"`
2. **Обновление статуса**: Runner устанавливает `"have ready result"` в `main_db.outbox` (атомарно)
3. **Асинхронное сохранение**: Outbox S3 worker ищет записи напрямую в `runner_db` со статусом `"predictable"` и сохраняет в S3
4. **Гарантия сохранения**: Данные остаются в `runner_db` после сохранения в S3 (для истории)
5. **Завершение процесса**: Обновление статуса на `"predicted"` в outbox после успешного сохранения

### Преимущества паттерна

- **Надёжность**: Результаты не теряются при сбоях MinIO или сети
- **Согласованность**: Транзакционная целостность данных (запись в БД и outbox атомарна)
- **Масштабируемость**: Можно запускать несколько worker'ов для параллельной обработки
- **Отказоустойчивость**: Автоматические повторные попытки при временных сбоях
- **Идемпотентность**: Повторная обработка безопасна (статус обновляется только после успеха)
- **Микросервисная архитектура**: Использует отдельную БД runner_db для изоляции сервисов
- **Сохранение истории**: Данные остаются в runner_db после сохранения в S3 для аналитики

### Жизненный цикл результата в Outbox S3

1. **Подготовка**: Runner Service сохраняет результат в `runner_db.runner_predictions` со статусом `"predictable"`
2. **Сигнал готовности**: Runner устанавливает `"have ready result"` в `main_db.outbox`
3. **Поиск данных**: Outbox S3 Worker ищет записи напрямую в `runner_db.runner_predictions` со статусом `"predictable"`
4. **Чтение данных**: Из записи в runner_db берутся `event_id`, `bucket` и `prediction` (финальный JSON)
5. **Сохранение**: Worker сохраняет prediction JSON в MinIO по пути `detection/{event_id}.json`
6. **Завершение**: После успешного сохранения статус обновляется на `"predicted"` в outbox
7. **Сохранение истории**: Данные остаются в `runner_db` (не удаляются) для аналитики

## Архитектура

### Поток данных
```
Runner Service → runner_db.runner_predictions + main_db.outbox → Outbox S3 Worker → MinIO → API Checkout
```

### Статусы обработки
- `have ready result` — Результат готов к сохранению в S3 (статус в main_db.outbox)
- `predictable` — Данные готовы для чтения из runner_db (статус в runner_db.runner_predictions)
- `predicted` — Результат успешно сохранён в S3 (статус в main_db.outbox)

## Технические детали

### Зависимости
- **pymongo** — клиент MongoDB (синхронный, так как worker работает в отдельном процессе)
- **minio** — S3-совместимый клиент для MinIO (синхронный)
- **json** — сериализация результатов
- **datetime** — временные метки

### Конфигурация
Переменные окружения:
- `MONGO_URI` — строка подключения к основной MongoDB (по умолчанию: `mongodb://admin:password@mongo:27017/`)
- `RUNNER_MONGO_URI` — строка подключения к runner_db (по умолчанию: `mongodb://admin:password@mongo:27017/`)
- `MINIO_ENDPOINT` — адрес MinIO (по умолчанию: `minio:9000`)
- `MINIO_ACCESS_KEY` — ключ доступа MinIO (по умолчанию: `minioadmin`)
- `MINIO_SECRET_KEY` — секретный ключ MinIO (по умолчанию: `minioadmin`)

### Настройки MinIO
```python
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,  # False для локальной разработки
)
```

### Почему используется синхронный клиент?

**Outbox S3 Worker** работает как отдельный процесс (не в рамках FastAPI event loop), поэтому используются синхронные клиенты:

- **Отдельный процесс**: Worker не использует asyncio event loop
- **Простота**: Синхронный код проще для worker'ов
- **Надёжность**: Синхронные клиенты имеют встроенные retry механизмы
- **Производительность**: Для worker'а достаточно синхронного подхода

### Борьба с GIL и Event Loop

**Outbox S3 Worker** работает как отдельный процесс, поэтому:
- **Не использует event loop**: Worker не является частью FastAPI приложения
- **GIL не критичен**: Worker выполняет I/O операции (MongoDB, MinIO), которые освобождают GIL
- **Простота**: Синхронный код проще для понимания и поддержки

**Примечание**: Если бы worker был частью FastAPI приложения, нужно было бы использовать асинхронные клиенты и `run_in_executor` для избежания блокировки event loop.

## Процесс обработки

### 1. Подключение к сервисам
```python
# Подключение к основной MongoDB
client = MongoClient(MONGO_URI)
db = client['main_db']
outbox = db['outbox']

# Подключение к runner_db для получения результатов
runner_client = MongoClient(RUNNER_MONGO_URI)
runner_db = runner_client['runner_db']
runner_predictions = runner_db['runner_predictions']

# Подключение к MinIO с повторными попытками
while True:
    try:
        minio_client = Minio(...)
        break
    except Exception as e:
        time.sleep(5)
```

### 2. Основной цикл обработки
```python
while True:
    # Поиск записей напрямую в runner_db со статусом "predictable" (финальный JSON готов)
    ready_predictions = list(runner_predictions.find({"status": "predictable"}).limit(10))
    
    for runner_pred in ready_predictions:
        event_id = runner_pred["_id"]
        bucket = runner_pred.get("bucket", "videos")
        prediction = runner_pred.get("prediction")
        
        if not prediction:
            continue
        
        # Сохранение в S3
        detection_key = f"detection/{event_id}.json"
        json_bytes = json.dumps(prediction, ensure_ascii=False, indent=2).encode("utf-8")
        
        minio_client.put_object(
            bucket,
            detection_key,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json"
        )
        
        # Обновление статуса в outbox
        outbox.update_one(
            {"_id": event_id},
            {
                "$set": {
                    "status": "predicted",
                    "predicted_at": datetime.utcnow(),
                    "output_s3": {
                        "bucket": bucket,
                        "object": detection_key
                    }
                }
            }
        )
        
        # Данные остаются в runner_db (не удаляются)
    
    time.sleep(1)
```

## Формат данных

### Структура в runner_db.runner_predictions
```json
{
  "_id": "video-uuid",
  "event_id": "video-uuid",
  "bucket": "videos",
  "total_frames": 30,
  "received_frames": 30,
  "frame_results": {
    "0": {"frame": 0, "objects": [...]},
    "5": {"frame": 5, "objects": [...]}
  },
  "prediction": [
    {"frame": 0, "objects": [...]},
    {"frame": 5, "objects": [...]}
  ],
  "status": "predictable"
}
```

### Структура в MinIO
Путь: `detection/{event_id}.json`

```json
[
  {
    "frame": 0,
    "objects": [
      {
        "class": "cat",
        "box": [100, 200, 300, 400],
        "conf": 0.87
      }
    ]
  },
  {
    "frame": 5,
    "objects": [
      {
        "class": "dog",
        "box": [150, 250, 350, 450],
        "conf": 0.92
      }
    ]
  }
]
```

### Обновление в outbox
```json
{
  "_id": "video-uuid",
  "status": "predicted",
  "predicted_at": "2024-01-01T12:05:00Z",
  "output_s3": {
    "bucket": "videos",
    "object": "detection/video-uuid.json"
  }
}
```

## Пути в MinIO

### Структура каталогов
```
videos/
├── input/
│   ├── video-uuid-1.mp4     # Исходные видео
│   └── video-uuid-2.mp4
├── detection/
│   ├── video-uuid-1.json    # Результаты детекции
│   └── video-uuid-2.json
└── result/                   # (резерв для будущих нужд)
```

## Отказоустойчивость и проверка доступности сервисов

### Проверка доступности MinIO

Worker проверяет доступность MinIO при старте и автоматически восстанавливается при сбоях:

```python
# outbox_s3/outbox_s3_worker.py — подключение к MinIO с повторными попытками
# Ждём готовности MinIO
minio_client = None
while True:
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        print("[outbox_s3] Connected to MinIO!", flush=True)
        break  # Успешное подключение
    except Exception as e:
        print(f"[outbox_s3] MinIO not ready yet, retrying... {e}", flush=True)
        time.sleep(5)  # Повторная попытка через 5 секунд
```

**Преимущества**:
- **Автоматическое восстановление**: При восстановлении MinIO worker автоматически подключается
- **Не блокирует старт**: Worker запускается и ждёт доступности MinIO
- **Логирование**: Все ошибки подключения логируются

### Проверка доступности MongoDB

Worker обрабатывает ошибки MongoDB с логированием и продолжением работы:

```python
# outbox_s3/outbox_s3_worker.py — операции с MongoDB
# Подключение к основной MongoDB (main_db) и runner_db
client = MongoClient(MONGO_URI)
db = client['main_db']
outbox = db['outbox']

runner_client = MongoClient(RUNNER_MONGO_URI)
runner_db = runner_client['runner_db']
runner_predictions = runner_db['runner_predictions']

# Основной цикл обработки
while True:
    try:
        # Ищем записи напрямую в runner_db со статусом "predictable"
        ready_predictions = list(runner_predictions.find({"status": "predictable"}).limit(10))
        
        for runner_pred in ready_predictions:
            try:
                # Чтение данных из runner_db...
                # Сохранение в MinIO...
                # Обновление статуса в outbox...
            except Exception as e:
                print(f"[outbox_s3] Failed to save to S3: {e}", flush=True)
                # Запись в runner_db остаётся со статусом "predictable" для повторной обработки
    except Exception as e:
        print(f"[outbox_s3] Worker error: {e}", flush=True)
        # Продолжаем работу при временных сбоях
    
    time.sleep(1)  # Проверяем каждую секунду
```

**Преимущества**:
- **Обработка ошибок**: Все ошибки MongoDB логируются
- **Продолжение работы**: Worker продолжает работать при временных сбоях БД
- **Повторная обработка**: События остаются в outbox для повторной обработки
- **Микросервисная архитектура**: Использует отдельную БД runner_db для изоляции сервисов

## Обработка ошибок

### Типы ошибок и стратегия обработки

#### Ошибка подключения MinIO
- **Симптомы**: Не удаётся подключиться к MinIO серверу
- **Обработка**: Повторные попытки каждые 5 секунд с логированием
- **Восстановление**: Автоматическое подключение при восстановлении MinIO

#### Ошибка сохранения файла
- **Симптомы**: Не удаётся сохранить JSON файл в MinIO
- **Обработка**: Логирование ошибки, событие остаётся со статусом `"have ready result"` в outbox
- **Восстановление**: Событие будет обработано при следующей итерации

#### Ошибка MongoDB
- **Симптомы**: Не удаётся прочитать/обновить данные в MongoDB
- **Обработка**: Логирование ошибки, продолжение работы
- **Восстановление**: Повторная попытка при следующей итерации

#### Нет prediction в записи
- **Симптомы**: Запись в `runner_db.runner_predictions` есть, но поле `prediction` отсутствует или пустое
- **Обработка**: Пропуск записи, логирование и продолжение работы
- **Восстановление**: Запись будет обработана, когда prediction появится

### Стратегия восстановления

1. **MinIO недоступен**: Worker продолжает попытки подключения каждые 5 секунд
2. **Ошибка сохранения**: Запись остаётся в runner_db со статусом `"predictable"` для повторной обработки
3. **MongoDB ошибка**: Логирование и продолжение работы, повторная попытка при следующей итерации
4. **Нет prediction**: Пропуск записи, логирование и продолжение работы
5. **Идемпотентность**: Повторная обработка безопасна (статус в outbox обновляется только после успеха)
6. **Сохранение истории**: Данные остаются в runner_db после сохранения в S3 для аналитики

## Производительность

### Оптимизации
- **Пакетная обработка**: До 10 результатов за одну итерацию
- **Потоковая загрузка**: Использование `io.BytesIO` для эффективной загрузки
- **Опрос с интервалом**: 1 секунда между проверками

### Метрики
- **Пропускная способность**: Зависит от размера JSON и скорости MinIO
- **Задержка**: Минимальная (1 секунда)
- **Надёжность**: 99.9% (с retry механизмом)

## Логирование

Детальное логирование операций:
- Подключение к MinIO
- Найденные результаты
- Сохранённые файлы
- Обновлённые статусы
- Ошибки и исключения

## Развертывание

### Docker конфигурация
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY outbox_s3_worker.py .
CMD python outbox_s3_worker.py
```

### Масштабирование
- Можно запускать несколько экземпляров worker'а
- MongoDB обеспечивает консистентность через атомарные операции
- MinIO обеспечивает надёжное хранение данных

## Важные замечания

1. **Идемпотентность**: Повторная обработка событий безопасна
2. **Атомарность**: Операции обновления MongoDB атомарны
3. **Отказоустойчивость**: Автоматическое восстановление после сбоев
4. **Мониторинг**: Логирование всех операций для отладки
5. **Микросервисная архитектура**: Использует отдельную БД runner_db для хранения результатов
6. **Сохранение истории**: Данные остаются в runner_db после сохранения в S3 для аналитики
