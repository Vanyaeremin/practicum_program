"""
S3 Utils — утилиты для работы с MinIO объектным хранилищем

Этот модуль предоставляет функции для работы с MinIO:
- Скачивание видео файлов из объектного хранилища
- Загрузка кадров в S3 бакет "frames" для передачи в inference сервис
- Проверка доступности S3/MinIO сервиса

Конфигурация:
- MINIO_ENDPOINT: адрес MinIO сервера
- MINIO_ACCESS_KEY: ключ доступа
- MINIO_SECRET_KEY: секретный ключ
- MINIO_SECURE: использование HTTPS (False для локальной разработки)
"""
import io
import os
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = False  

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def download_image(bucket: str, object_name: str) -> bytes:
    response = client.get_object(bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return data


def upload_frame(bucket: str, object_name: str, frame_bytes: bytes) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.put_object(
        bucket,
        object_name,
        data=io.BytesIO(frame_bytes),
        length=len(frame_bytes),
        content_type="image/jpeg"
    )


def check_s3_availability(service_name: str = "runner") -> bool:
    try:
        client.list_buckets()
        return True
    except Exception as e:
        print(f"[{service_name}] S3/MinIO not available: {e}", flush=True)
        try:
            new_client = recreate_client()
            new_client.list_buckets()
            return True
        except Exception as reinit_error:
            print(f"[{service_name}] Reinitialization failed: {reinit_error}", flush=True)
            return False


def recreate_client():
    global client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return client