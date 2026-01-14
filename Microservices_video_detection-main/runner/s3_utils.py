"""
S3 Utils — утилиты для работы с MinIO объектным хранилищем

Этот модуль предоставляет функции для скачивания файлов из MinIO.
Используется в runner сервисе для скачивания видео файлов из объектного хранилища.

Конфигурация:
- MINIO_ENDPOINT: адрес MinIO сервера
- MINIO_ACCESS_KEY: ключ доступа
- MINIO_SECRET_KEY: секретный ключ
- MINIO_SECURE: использование HTTPS (False для локальной разработки)
"""
from minio import Minio
import os

# Конфигурация MinIO из переменных окружения
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False  # False для HTTP в локальной разработке

# Инициализация MinIO клиента
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

def download_image(bucket: str, object_name: str) -> bytes:
    """
    Скачивает файл из MinIO и возвращает его в виде байтов.
    
    Используется в runner сервисе для скачивания видео файлов из MinIO
    перед извлечением кадров.
    
    Args:
        bucket: имя бакета в MinIO (обычно "videos")
        object_name: путь к файлу в бакете (например, "input/video-uuid.mp4")
    
    Returns:
        bytes: содержимое файла в виде байтов
    
    Raises:
        Exception: при ошибке подключения или отсутствия файла
    
    Пример:
        video_bytes = download_image("videos", "input/123e4567-e89b-12d3-a456-426614174000.mp4")
    """
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return data

def recreate_client():
    """Пересоздаёт MinIO клиент"""
    global client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client