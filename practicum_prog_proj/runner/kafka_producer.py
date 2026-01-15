"""
Kafka Producer — асинхронный продюсер для отправки задач обработки кадров в inference сервис

Этот модуль предоставляет обёртку над AIOKafkaProducer для отправки
задач обработки кадров из runner сервиса в inference сервис через Kafka.

Используется для отправки задач обработки отдельных кадров (каждый 5-й кадр видео)
в Kafka тему "inference_tasks" для последующей детекции объектов.

Важно: Кадры сохраняются в S3 бакет "frames", в Kafka отправляется только путь к кадру.

Класс RunnerKafkaProducer обеспечивает:
- Асинхронное подключение к Kafka
- Сериализацию сообщений в JSON
- Надёжную отправку с ожиданием подтверждения
- Управление жизненным циклом продюсера
"""
import json
from aiokafka import AIOKafkaProducer


class RunnerKafkaProducer:
    def __init__(self, bootstrap_servers='kafka:9092', topic='inference_tasks'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await self.producer.start()
        print(f"[runner] Kafka producer started for topic: {self.topic}", flush=True)

    async def send(self, value):
        if not self.producer:
            await self.start()

        event_id = value.get('event_id')
        key = event_id.encode() if event_id else None

        await self.producer.send_and_wait(self.topic, value, key=key)
        print(
            f"[runner] Produced inference job to Kafka: "
            f"frame_id={value.get('frame_id')}, event_id={value.get('event_id')}, "
            f"key={key}",
            flush=True
        )

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            print(f"[runner] Kafka producer stopped for topic: {self.topic}", flush=True)
