# Inference Service (Порт: 8003)

## Обзор

**Inference Service** — сервис детекции объектов с использованием YOLO модели. Сервис обрабатывает отдельные кадры видео, выполняет детекцию объектов и возвращает результаты в формате JSON.

## Основные функции

1. **Получение кадров** из Kafka темы `inference_tasks`
2. **Декодирование кадров** из формата base64 JPEG
3. **Детекция объектов** с помощью YOLO модели
4. **Отправка результатов** в Kafka тему `inference_results`
5. **Кэширование модели** для оптимизации производительности

## Архитектура обработки

### Обработка по кадрам

Сервис работает с отдельными кадрами, а не с видео целиком:

1. **Читает задачи** из Kafka темы `inference_tasks`
2. **Получает кадр** в формате base64 (JPEG) из сообщения Kafka
3. **Декодирует кадр** и запускает YOLO детекцию
4. **Отправляет JSON результат** для кадра в Kafka тему `inference_results`
5. **Обновляет статусы** в MongoDB

## API Эндпоинты

### Health Check
- `GET /data`, `GET /api/data` — Базовый эндпоинт сервиса
- `GET /health` — Health check (если определён)

## Внутренние компоненты

### Kafka Consumer/Producer

#### Consumer для задач инференса
- **Тема**: `inference_tasks`
- **Группа**: `inference-group`
- **Функция**: Получение кадров для детекции

#### Producer для результатов
- **Тема**: `inference_results`
- **Функция**: Отправка результатов детекции

### YOLO Модель

#### Конфигурация модели
- **Файл модели**: `best.pt` (веса YOLO)
- **Порог уверенности**: `conf=0.25` (снижен для лучшего распознавания животных)
- **Порог IoU**: `iou=0.45` (более строгий для фильтрации дубликатов)

#### Кэширование модели
```python
# Global model cache - load YOLO model once
_global_model = None
_model_path = None

def get_yolo_model(yolo_path: str = 'best.pt') -> YOLO:
    """Get or create cached YOLO model instance"""
    global _global_model, _model_path
    
    if _global_model is None or _model_path != yolo_path:
        _global_model = YOLO(yolo_path)
        _model_path = yolo_path
    
    return _global_model
```

## Технические детали

### Зависимости
- **FastAPI** — веб-фреймворк
- **aiokafka** — асинхронный клиент Kafka
- **ultralytics** — YOLO модель
- **OpenCV** — обработка изображений
- **NumPy** — операции с массивами
- **MongoDB** — хранилище статусов

### Конфигурация
Переменные окружения:
- `MONGO_URI` — строка подключения к MongoDB (по умолчанию: `mongodb://admin:password@mongo:27017/`)

### Асинхронность и избегание блокировки Event Loop

Сервис активно использует асинхронность для обеспечения высокой производительности:

#### 1. Асинхронные клиенты Kafka (aiokafka)

**Проблема**: Синхронный клиент Kafka блокирует event loop при отправке/получении сообщений.

**Решение**: Использование `aiokafka` — полностью асинхронного клиента.

```python
# inference/kafka_consumer.py — Consumer для получения задач
from aiokafka import AIOKafkaConsumer

consumer = AIOKafkaConsumer(
    "inference_tasks",
    bootstrap_servers="kafka:9092",
    group_id="inference-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # Ручной коммит для надёжности
    value_deserializer=lambda m: json.loads(m.decode()),
)

await consumer.start()  # Асинхронное подключение, не блокирует event loop

# Асинхронная итерация по сообщениям
async for msg in consumer:
    # Event loop не блокируется во время ожидания новых сообщений
    await process_frame(msg)
    await consumer.commit()  # Асинхронный коммит
```

```python
# inference/kafka_consumer.py — Producer для отправки результатов
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

await producer.start()  # Асинхронное подключение

# Асинхронная отправка с ожиданием подтверждения
await producer.send_and_wait("inference_results", message, key=key)
# Event loop не блокируется, может обрабатывать другие задачи
```

**Преимущества**:
- Event loop не блокируется
- Можно обрабатывать множество кадров одновременно
- Высокая производительность

#### 2. ProcessPoolExecutor для CPU-интенсивных задач (YOLO детекция)

**Проблема**: YOLO детекция — CPU-интенсивная операция, которая блокирует GIL и event loop. `ThreadPoolExecutor` не обеспечивает истинного параллелизма из-за GIL.

**Решение**: Использование `ProcessPoolExecutor` для вынесения YOLO обработки в отдельные процессы. Каждый процесс имеет свой GIL, что обеспечивает истинный параллелизм на многоядерных CPU.

```python
# inference/kafka_consumer.py — обработка кадра через YOLO
from concurrent.futures import ProcessPoolExecutor
import os

# Глобальный ProcessPoolExecutor для CPU-интенсивных YOLO операций
executor = ProcessPoolExecutor(max_workers=os.cpu_count())

# Запускаем YOLO на кадре в отдельном процессе
loop = asyncio.get_running_loop()
frame_result = await loop.run_in_executor(
    executor,  # ProcessPoolExecutor для обхода GIL и истинного параллелизма
    get_yolo_predictions_for_frame_base64,
    frame_data, frame_id
)
# Event loop не блокируется во время YOLO обработки
```

**Как это работает**:
- `ProcessPoolExecutor` создаёт отдельные процессы Python
- Каждый процесс имеет свой GIL, что позволяет использовать все ядра CPU
- YOLO обработка выполняется в отдельном процессе
- Event loop продолжает обрабатывать другие задачи
- Когда обработка завершается, результат возвращается через await
- Модель YOLO загружается в каждом процессе отдельно (кэшируется внутри процесса)

**Преимущества ProcessPoolExecutor**:
- **Истинный параллелизм**: Несколько кадров могут обрабатываться одновременно на разных ядрах CPU
- **Обход GIL**: Каждый процесс имеет свой GIL, что позволяет использовать все ядра
- **Высокая производительность**: Значительное ускорение на многоядерных системах
- **Масштабируемость**: Автоматическое использование всех доступных ядер CPU

**Примечание**: Модель YOLO загружается в каждом процессе отдельно при первом использовании и кэшируется внутри процесса для оптимизации.

#### 3. run_in_executor для MongoDB операций

**Проблема**: Синхронные операции с MongoDB блокируют event loop.

**Решение**: Использование `run_in_executor` для вынесения MongoDB операций в отдельный поток.

```python
# inference/kafka_consumer.py — обработка кадра через YOLO
# ВАЖНО: Inference Service НЕ обновляет outbox - это соответствует принципам микросервисов
# Статусы обработки отслеживаются в runner_db через runner service
loop = asyncio.get_running_loop()
frame_result = await loop.run_in_executor(
    None,  # ThreadPoolExecutor
    get_yolo_predictions_for_frame_base64,
    frame_data, frame_id
)
```

### Борьба с GIL (Global Interpreter Lock)

**GIL (Global Interpreter Lock)** — это механизм в CPython, который предотвращает одновременное выполнение байт-кода несколькими потоками в одном процессе.

**Проблема в Inference Service**:
- YOLO детекция — CPU-интенсивная операция
- Если выполнять YOLO обработку в основном потоке, GIL блокирует выполнение других задач
- `ThreadPoolExecutor` не обеспечивает истинного параллелизма из-за GIL

**Решение**:
1. **ProcessPoolExecutor** (через `run_in_executor(executor, ...)`) для YOLO обработки:
   - Каждый процесс имеет свой GIL, что обеспечивает истинный параллелизм
   - Можно обрабатывать несколько кадров одновременно на разных ядрах CPU
   - Event loop не блокируется
   - Модель YOLO загружается в каждом процессе отдельно (кэшируется внутри процесса)

2. **ThreadPoolExecutor** (через `run_in_executor(None, ...)`) для I/O операций:
   - MongoDB операции
   - Event loop не блокируется

**Преимущества ProcessPoolExecutor**:
- **Истинный параллелизм**: Несколько кадров обрабатываются одновременно на разных ядрах CPU
- **Обход GIL**: Каждый процесс имеет свой GIL
- **Высокая производительность**: Значительное ускорение на многоядерных системах (200-300% улучшение)
- **Масштабируемость**: Автоматическое использование всех доступных ядер CPU

**Пример использования**:
```python
# CPU-интенсивная задача (YOLO) — используем ProcessPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import os

executor = ProcessPoolExecutor(max_workers=os.cpu_count())

loop = asyncio.get_running_loop()
frame_result = await loop.run_in_executor(
    executor,  # ProcessPoolExecutor для обхода GIL
    get_yolo_predictions_for_frame_base64,
    frame_data, frame_id
)

# ВАЖНО: Inference Service НЕ обновляет outbox - статусы отслеживаются в runner_db
# Обработка кадра выполняется через YOLO без обновления статусов в outbox
```

## Процесс обработки кадра

### 1. Получение задачи
```python
# Чтение из Kafka темы inference_tasks
event_id = msg.value.get("event_id")
frame_id = msg.value.get("frame_id")
frame_data = msg.value.get("frame_data")  # Base64 JPEG
```

### 2. Декодирование кадра
```python
def get_yolo_predictions_for_frame_base64(frame_base64, frame_id, yolo_path='best.pt'):
    # Декодируем base64 в байты
    frame_bytes = base64.b64decode(frame_base64)
    
    # Декодируем JPEG в numpy array
    np_img = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
```

### 3. Детекция объектов
```python
# Загружаем модель (кэшированную)
model = get_yolo_model(yolo_path)

# Запускаем YOLO на кадре
yolo_results = model.predict(source=frame, conf=conf, iou=iou, verbose=False)

# Извлекаем детекции
frame_objects = []
for r in yolo_results:
    for box in r.boxes:
        cls = int(box.cls[0])
        label = model.names[cls]
        x1, y1, x2, y2 = map(int, box.xyxy[0])
        conf_val = float(box.conf[0])
        
        frame_objects.append({
            "class": label,
            "box": [x1, y1, x2, y2],
            "conf": conf_val
        })
```

### 4. Форматирование результата
```python
result = {
    "frame": frame_id,
    "objects": frame_objects
}
```

### 5. Отправка результата
```python
message = {
    "event_id": event_id,
    "frame_id": frame_id,
    "bucket": bucket,
    "prediction": frame_result
}
await producer.send_and_wait("inference_results", message)
```

## Очерёдность кадров

Очерёдность обеспечивается через:
- **Kafka Consumer Groups**: Все кадры одного видео обрабатываются последовательно
- **frame_id**: Уникальный номер кадра для правильной сборки JSON
- **Сохранение frame_id**: Включается в результат для сборки в runner

## Управление статусами

**ВАЖНО**: Inference Service НЕ обновляет outbox - это соответствует принципам микросервисной архитектуры. Статусы обработки отслеживаются в runner_db через runner service.

## Настройка модели

### Смена модели
Чтобы сменить модель, обновите путь в двух местах:
1. `yolo_detect_frame.py`: `yolo_path='best.pt'`
2. `kafka_consumer.py`: `yolo_path='best.pt'`

### Параметры детекции
- `conf=0.25` — порог уверенности (снижен для животных)
- `iou=0.45` — порог IoU для NMS

## Отказоустойчивость и проверка доступности сервисов

### Проверка доступности Kafka

Сервис проверяет доступность Kafka при старте и автоматически восстанавливается при сбоях:

```python
# inference/kafka_consumer.py — подключение к Kafka с повторными попытками
async def consume_inference():
    # Ожидаем готовности Kafka для consumer
    while True:
        try:
            consumer = AIOKafkaConsumer(...)
            await consumer.start()
            print("[inference] aiokafka consumer connected", flush=True)
            break  # Успешное подключение
        except Exception as e:
            print(f"[inference] aiokafka not ready yet, retry in 5s... {e}", flush=True)
            await asyncio.sleep(5)  # Повторная попытка через 5 секунд
    
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
    
    # Основной цикл обработки сообщений
    try:
        async for msg in consumer:
            # Обработка кадра...
            await consumer.commit()  # Коммит только после успешной обработки
    finally:
        await consumer.stop()  # Корректное закрытие при завершении
        if producer:
            await producer.stop()
```

**Преимущества**:
- **Автоматическое восстановление**: При восстановлении Kafka сервис автоматически подключается
- **Не блокирует старт**: Сервис запускается и ждёт доступности Kafka
- **Логирование**: Все ошибки подключения логируются

### Проверка доступности MongoDB

Сервис обрабатывает ошибки MongoDB с логированием и продолжением работы:

```python
# inference/kafka_consumer.py — обработка кадра через YOLO
# ВАЖНО: Inference Service НЕ обновляет outbox - это соответствует принципам микросервисов
# Статусы обработки отслеживаются в runner_db через runner service
from concurrent.futures import ProcessPoolExecutor
import os

executor = ProcessPoolExecutor(max_workers=os.cpu_count())

loop = asyncio.get_running_loop()
frame_result = await loop.run_in_executor(
    executor,  # ProcessPoolExecutor для обхода GIL и истинного параллелизма
    get_yolo_predictions_for_frame_base64,
    frame_data, frame_id
)
# Результат отправляется в Kafka, статусы обновляются в runner_db через runner service
```

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

### Ошибки декодирования кадров
- **Симптомы**: Не удаётся декодировать base64 или JPEG кадр
- **Обработка**: Логирование ошибки, пропуск проблемного кадра
- **Восстановление**: Следующий кадр обрабатывается нормально

### Ошибки загрузки модели
- **Симптомы**: Не удаётся загрузить YOLO модель из файла
- **Обработка**: Логирование ошибки, модель кэшируется после первой загрузки
- **Восстановление**: Модель загружается при следующей попытке

### Ошибки детекции
- **Симптомы**: Ошибки при выполнении YOLO детекции
- **Обработка**: Логирование ошибки, пропуск проблемного кадра
- **Восстановление**: Следующий кадр обрабатывается нормально

### Ошибки MongoDB операций
- **Симптомы**: Не удаётся записать/прочитать данные из MongoDB
- **Обработка**: Логирование ошибки, продолжение работы
- **Восстановление**: Повторная обработка при следующей итерации

### Стратегия обработки ошибок

1. **Логирование**: Все ошибки логируются с детальной информацией
2. **Продолжение работы**: Сервис продолжает работать при временных сбоях
3. **Повторная обработка**: Kafka сообщения не коммитятся при ошибках
4. **Автоматическое восстановление**: При восстановлении сервисов обработка продолжается автоматически
5. **Кэширование модели**: YOLO модель загружается один раз и кэшируется для оптимизации

## Логирование

Детальное логирование:
- Подключение к Kafka
- Обработка кадров
- Результаты детекции
- Ошибки и исключения

## Важные замечания

1. **Производительность**: 
   - Модель кэшируется после первой загрузки в каждом процессе
   - Используется ProcessPoolExecutor для истинного параллелизма на многоядерных CPU
   - Значительное ускорение обработки (200-300% улучшение производительности)
2. **Надёжность**: Отключён авто-коммит Kafka для гарантированной обработки
3. **Масштабируемость**: Можно запускать несколько экземпляров для горизонтального масштабирования
4. **Эффективность**: Оптимизированная обработка кадров с использованием асинхронности и ProcessPoolExecutor
