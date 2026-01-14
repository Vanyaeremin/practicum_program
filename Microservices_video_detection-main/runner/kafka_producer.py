"""
Kafka Producer — асинхронный продюсер для отправки кадров в inference сервис

Этот модуль предоставляет обёртку над AIOKafkaProducer для отправки
предобработанных кадров из runner сервиса в inference сервис через Kafka.

Используется для отправки отдельных кадров (каждый 5-й кадр видео)
в Kafka тему "inference_tasks" для последующей детекции объектов.

Класс RunnerKafkaProducer обеспечивает:
- Асинхронное подключение к Kafka
- Сериализацию сообщений в JSON
- Надёжную отправку с ожиданием подтверждения
- Управление жизненным циклом продюсера
"""
import json
import asyncio
from aiokafka import AIOKafkaProducer

class RunnerKafkaProducer:
    """
    Асинхронный продюсер Kafka для отправки кадров в inference сервис.
    
    Обеспечивает отправку предобработанных кадров в Kafka тему "inference_tasks"
    для последующей детекции объектов YOLO моделью.
    """
    
    def __init__(self, bootstrap_servers='kafka:9092', topic='inference_tasks'):
        """
        Инициализация продюсера Kafka.
        
        Args:
            bootstrap_servers: адрес Kafka брокеров (по умолчанию 'kafka:9092')
            topic: тема Kafka для отправки сообщений (по умолчанию 'inference_tasks')
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def start(self):
        """
        Запускает продюсер Kafka и устанавливает соединение.
        
        Создаёт экземпляр AIOKafkaProducer с сериализацией сообщений в JSON.
        """
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await self.producer.start()
        print(f"[runner] Kafka producer started for topic: {self.topic}", flush=True)

    async def send(self, value):
        """
        Отправляет сообщение в Kafka с ожиданием подтверждения.
        
        Args:
            value: словарь с данными кадра для отправки в inference
            
        Пример сообщения:
            {
                "event_id": "video-uuid",
                "frame_id": 0,
                "bucket": "videos",
                "frame_data": "base64-encoded-jpeg",
                "media_type": "frame"
            }
        """
        if not self.producer:
            await self.start()
        
        # Используем event_id как ключ для гарантии, что все кадры одного видео попадут в одну partition
        event_id = value.get('event_id')
        key = event_id.encode() if event_id else None
        
        # Отправляем сообщение с ключом partitioning и ждём подтверждения
        await self.producer.send_and_wait(self.topic, value, key=key)
        print(f"[runner] Produced inference job to Kafka: frame_id={value.get('frame_id')}, event_id={value.get('event_id')}, key={key}", flush=True)

    async def stop(self):
        """
        Останавливает продюсер Kafka и закрывает соединения.
        
        Вызывать при завершении работы сервиса для корректного освобождения ресурсов.
        """
        if self.producer:
            await self.producer.stop()
            print(f"[runner] Kafka producer stopped for topic: {self.topic}", flush=True)
