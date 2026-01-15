"""
Inference Service — сервис для детекции объектов

Сервис предоставляет веб-интерфейс и запускает фоновый Kafka consumer
для обработки кадров видео с использованием YOLO модели.

Основные функции:
1. Запуск Kafka consumer при старте сервиса
2. Обеспечение работы фонового процесса детекции объектов

Асинхронная обработка кадров выполняется в kafka_consumer.py
"""
from fastapi import FastAPI

from kafka_consumer import run_bg_consumer

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    run_bg_consumer()


@app.get("/data")
@app.get("/api/data")
def get_data():
    return {"service": "inference-api", "message": "Hello from Inference Service"}