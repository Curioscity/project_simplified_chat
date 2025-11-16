import logging
from datetime import datetime, UTC
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from orjson import orjson

logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    """Kafka 인프라 어댑터 - 연결 및 메시지 스트림 제공"""

    def __init__(
        self,
        topic: str = "chat-messages",
        bootstrap_servers: str = "kafka:9092",
        group_id: str = "chat-batch-v1",
    ):
        self._consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=500,
            fetch_max_wait_ms=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )

    async def start(self):
        """Kafka Consumer 시작"""
        await self._consumer.start()
        logger.info(f"KafkaMessageConsumer started")

    async def stop(self):
        """Kafka Consumer 종료"""
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka adapter stopped")

    async def listen(self) -> AsyncIterator[dict]:
        """메시지 스트림 제공 (파싱 포함)"""
        if not self._consumer:
            raise RuntimeError("Kafka adapter not started")

        async for message in self._consumer:
            try:
                data = orjson.loads(message.value.decode("utf-8"))
                yield data
            except Exception as e:
                logger.error(f"Error parsing Kafka message: {e}", exc_info=True)

    async def commit(self):
        """오프셋 커밋"""
        if self._consumer:
            await self._consumer.commit()


class KafkaProducerAdapter:
    """Kafka Producer 어댑터"""

    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        acks: str | int = -1,
        enable_idempotence: bool = True,
        max_batch_size: int = 16384 * 10,
        linger_ms: int = 10,
        retry_backoff_ms: int = 1000,
    ):
        self._producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            enable_idempotence=enable_idempotence,
            max_batch_size=max_batch_size,
            linger_ms=linger_ms,
            compression_type="gzip",
            retry_backoff_ms=retry_backoff_ms,
        )

    async def start(self):
        """Kafka Producer 시작"""
        await self._producer.start()
        logger.info("Kafka producer started")

    async def stop(self):
        """Kafka Producer 종료"""
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def send_chat_message_to_dlq(
        self,
        message: dict,
        error_reason: str,
    ):
        try:
            dlq_message = {
                **message,
                "dlq_metadata": {
                    "error_reason": error_reason,
                    "failed_at": datetime.now(UTC).isoformat(),
                },
            }

            await self._producer.send(
                "chat-messages-dlq",
                key=message.get("room_id").encode("utf-8"),
                value=orjson.dumps(dlq_message),
            )

            logger.warning(
                f"Message sent to DLQ",
                extra={
                    "room_id": message.get("room_id"),
                    "user_id": message.get("user_id"),
                    "error_reason": error_reason,
                },
            )

        except KafkaError as e:
            logger.error(
                f"Failed to send to DLQ: {e}",
                exc_info=True,
                extra={
                    **message,
                    "error_reason": error_reason,
                },
            )
