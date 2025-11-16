import asyncio
import logging

from src.infrastructure.kafka import (
    KafkaMessageConsumer,
    KafkaProducerAdapter,
)
from src.infrastructure.message_repository import MessageRepository
from src.infrastructure.otel import OTELManager

logger = logging.getLogger(__name__)


class MessageStorageProcessor:
    """메시지 수신 → 배치 → 저장 전체 흐름 관리 (None 없이 stop_event 기반)"""

    def __init__(
        self,
        otel_manager: OTELManager,
        kafka_message_consumer: KafkaMessageConsumer,
        message_repository: MessageRepository,
        kafka_producer_adapter: KafkaProducerAdapter,
        max_batch_size: int = 100,
        flush_interval: float = 1.0,
        shutdown_timeout: float = 10.0,
    ):
        self.kafka_message_consumer = kafka_message_consumer
        self.kafka_producer_adapter = kafka_producer_adapter
        self.repository = message_repository
        self.otel_manager = otel_manager
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self.shutdown_timeout = shutdown_timeout

        self._batch_queue: asyncio.Queue = asyncio.Queue()
        self._listener_task: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self):
        """프로세서 시작"""
        if self._stop_event.is_set():
            self._stop_event.clear()

        self._listener_task = asyncio.create_task(self._listener_worker())
        self._flush_task = asyncio.create_task(self._flush_worker())

        logger.info("MessageStorageProcessor started")

    async def stop(self):
        """Graceful shutdown: 모든 메시지 처리 후 종료"""
        if self._stop_event.is_set():
            logger.info("MessageStorageProcessor already stopping/stopped")
            return

        logger.info("MessageStorageProcessor stopping...")
        self._stop_event.set()

        # 1. Listener 자연 종료 유도
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await asyncio.wait_for(self._listener_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # 2. Flush worker 자연 종료 대기
        done, pending = await asyncio.wait(
            [self._flush_task],
            timeout=self.shutdown_timeout,
            return_when=asyncio.ALL_COMPLETED,
        )

        if pending:
            logger.warning("Flush worker timeout, forcing cancel")
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        # 3. 큐에 남은 메시지 확인 및 경고
        remaining = self._batch_queue.qsize()
        if remaining > 0:
            logger.error(
                f"{remaining} messages left unprocessed in batch queue on shutdown"
            )

        logger.info("MessageStorageProcessor stopped")

    async def _listener_worker(self):
        """Kafka 메시지 수신 → 큐에 적재"""
        try:
            async for data in self.kafka_message_consumer.listen():
                if self._stop_event.is_set():
                    logger.info("Listener detected shutdown, stopping consumption")
                    break

                try:
                    message_doc = {
                        "room_id": data["room_id"],
                        "user_id": data["user_id"],
                        "content": data["content"],
                        "timestamp": data["timestamp"],
                    }
                    await self._batch_queue.put(message_doc)

                except KeyError as e:
                    logger.error(f"Missing required field: {e}", extra={"data": data})

                except Exception as e:
                    logger.error(
                        f"Error enqueueing message: {e}",
                        extra={"data": data},
                        exc_info=True,
                    )

        except asyncio.CancelledError:
            logger.info("Listener task cancelled")
            if not self._stop_event.is_set():
                raise
        except Exception as e:
            logger.critical(f"Listener critical error: {e}", exc_info=True)
            raise
        finally:
            logger.debug("Listener worker exited")

    async def _flush_worker(self):
        """배치 수집 및 처리"""
        batch: list[dict] = []

        async def flush_batch():
            if batch:
                await self._flush_batch(batch.copy())
                batch.clear()

        try:
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        self._batch_queue.get(), timeout=self.flush_interval
                    )
                    batch.append(message)
                    self._batch_queue.task_done()
                except asyncio.TimeoutError:
                    await flush_batch()
                    continue

                try:
                    async with asyncio.timeout(self.flush_interval):
                        while (
                            len(batch) < self.max_batch_size
                            and not self._stop_event.is_set()
                        ):
                            msg = self._batch_queue.get_nowait()
                            batch.append(msg)
                            self._batch_queue.task_done()
                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    pass

                await flush_batch()

            logger.info("Flush worker draining remaining messages...")

            while True:
                try:
                    msg = self._batch_queue.get_nowait()
                    batch.append(msg)
                    self._batch_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            if batch:
                await self._flush_batch(batch)
                logger.info(f"Flushed {len(batch)} remaining messages on shutdown")

            logger.info("Flush worker exited cleanly")

        except asyncio.CancelledError:
            logger.warning("Flush worker cancelled, flushing current batch...")
            await flush_batch()
            raise
        except Exception as e:
            logger.error(f"Flush worker error: {e}", exc_info=True)
            await flush_batch()
            raise
        finally:
            logger.debug("Flush worker terminated")

    async def _flush_batch(self, batch: list[dict]):
        """배치 저장 및 커밋"""
        if not batch:
            return

        try:
            with self.otel_manager.tracer.start_as_current_span(
                "kafka.batch.process",
                attributes={
                    "batch_size": len(batch),
                },
            ):
                result = await self.repository.save_messages(batch)

                # DLQ 처리
                if failed_messages := result.get("failed_messages", []):
                    for msg in failed_messages:
                        await self.kafka_producer_adapter.send_chat_message_to_dlq(
                            msg, error_reason="mongodb_write_failed"
                        )
                    logger.error(
                        f"{len(failed_messages)} messages sent to DLQ",
                        extra={"failed_count": len(failed_messages)},
                    )

                if result.get("inserted") or result.get("duplicates"):
                    await self.kafka_message_consumer.commit()
                    logger.info(
                        f"Flush success: {result.get('inserted', 0)} inserted, "
                        f"{result.get('duplicates', 0)} duplicates, {len(failed_messages)} failed"
                    )
                else:
                    logger.warning("No messages processed; commit skipped")

        except Exception as e:
            logger.error(f"Flush batch failed: {e}", exc_info=True)
            span = self.otel_manager.tracer.get_current_span()
            if span:
                span.record_exception(e)
