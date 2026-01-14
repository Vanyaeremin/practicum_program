"""
Inference Service — основной FastAPI сервис для детекции объектов

Сервис предоставляет веб-интерфейс и запускает фоновый Kafka consumer
для обработки кадров видео с использованием YOLO модели.

Основные функции:
1. Запуск Kafka consumer при старте сервиса
2. Предоставление базовых API эндпоинтов
3. Обеспечение работы фонового процесса детекции объектов

Асинхронная обработка кадров выполняется в kafka_consumer.py
"""
from fastapi import FastAPI
import httpx
from kafka_consumer import run_bg_consumer

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """
    Запускает фоновый Kafka consumer при старте сервиса.
    
    Consumer начинает слушать Kafka тему 'inference_tasks' и обрабатывать
    кадры видео для детекции объектов с помощью YOLO модели.
    """
    run_bg_consumer()

@app.get("/data")
@app.get("/api/data")
def get_data():
    """
    Базовый эндпоинт для проверки работы сервиса.
    
    Returns:
        dict: Информация о сервисе
    """
    return {"service": "inference-api", "message": "Hello from Inference Service"}