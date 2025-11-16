import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from orjson import orjson

logger = logging.getLogger(__name__)


class KafkaProducerAdapter:
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        acks: str | int = -1,
        enable_idempotence: bool = True,
        max_batch_size: int = 16384 * 10,
        linger_ms: int = 10,
        retry_backoff_ms: int = 1000,
    ):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            enable_idempotence=enable_idempotence,
            max_batch_size=max_batch_size,
            linger_ms=linger_ms,
            compression_type="gzip",
            retry_backoff_ms=retry_backoff_ms,
        )

    async def start(self):
        await self._producer.start()
        logger.info("Kafka producer started")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
        logger.info("Kafka producer stopped")

    async def send_chat_message(self, chat_message: dict):
        try:
            await self._producer.send(
                "chat-messages",
                key=chat_message["room_id"].encode("utf-8"),
                value=orjson.dumps(chat_message),
            )
        except KafkaError as e:
            logger.error(
                f"Kafka send error: {e}",
                extra={**chat_message},
            )
            raise
