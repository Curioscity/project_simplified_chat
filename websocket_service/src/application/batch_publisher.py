import asyncio
import logging

from src.application.exceptions import (
    BatchPublisherFullError,
    ServiceUnavailableError,
)
from src.infrastructure.otel import OTELManager
from src.infrastructure.redis import RedisAdapter

logger = logging.getLogger(__name__)


class BatchPublisher:
    """메시지를 버퍼링하여 배치로 Redis에 publish"""

    def __init__(
        self,
        otel_manager: OTELManager,
        redis_adapter: RedisAdapter,
        flush_interval: float = 0.01,
        max_queue_size: int = 10_000,
        queue_add_timeout: float = 1.0,
        max_batch_size: int = 30,
        num_partitions: int = 10,
    ):
        self.otel_manager = otel_manager
        self.redis_adapter = redis_adapter
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.queue_add_timeout = queue_add_timeout
        self.max_batch_size = max_batch_size
        self.num_partitions = num_partitions

        self._partition_queues: list[asyncio.Queue] = []
        self._partition_tasks: list[asyncio.Task] = []

        self._stop_event = asyncio.Event()

    async def start(self):
        for i in range(self.num_partitions):
            queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._partition_queues.append(queue)
            worker = asyncio.create_task(self._partition_worker(i, queue))
            self._partition_tasks.append(worker)

        logger.info("BatchPublisher started")

    async def stop(self, shutdown_timeout: float = 5.0):
        """종료 시 모든 큐의 메시지를 완전히 처리"""
        logger.info("BatchPublisher stopping...")
        self._stop_event.set()

        # 모든 워커가 정상 종료될 때까지 대기
        done, pending = await asyncio.wait(
            self._partition_tasks,
            timeout=shutdown_timeout,
            return_when=asyncio.ALL_COMPLETED,
        )

        if pending:
            logger.warning(f"{len(pending)} workers still running after timeout")

            # 남은 메시지 카운트 확인
            total_remaining = sum(q.qsize() for q in self._partition_queues)
            if total_remaining > 0:
                logger.warning(f"{total_remaining} messages remaining in queues")

            # 강제 취소
            for worker in pending:
                worker.cancel()

            # 취소 완료 대기
            await asyncio.gather(*pending, return_exceptions=True)

        # 최종 확인
        total_remaining = sum(q.qsize() for q in self._partition_queues)
        if total_remaining > 0:
            logger.error(f"Failed to flush {total_remaining} messages before shutdown")
        else:
            logger.info("All messages flushed successfully")

        logger.info("BatchPublisher stopped")

    async def _partition_worker(self, partition_id: int, queue: asyncio.Queue):
        """파티션별 워커 - 종료 시 큐를 완전히 비움"""
        batch: list[tuple[str, bytes]] = []

        async def flush_batch():
            nonlocal batch
            if batch:
                await self._flush(batch, partition_id)
                batch = []

        async def drain_remaining_queue():
            """큐에 남은 모든 메시지 수집 & flush"""
            remaining_messages = []
            while not queue.empty():
                try:
                    message = queue.get_nowait()
                    remaining_messages.append(message)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

            if remaining_messages:
                await self._flush(remaining_messages, partition_id)
                return len(remaining_messages)
            return 0

        try:
            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        queue.get(), timeout=self.flush_interval
                    )
                    batch.append(message)
                    queue.task_done()
                except asyncio.TimeoutError:
                    await flush_batch()
                    continue

                try:
                    async with asyncio.timeout(self.flush_interval):
                        while len(batch) < self.max_batch_size:
                            message = queue.get_nowait()
                            batch.append(message)
                            queue.task_done()
                except asyncio.QueueEmpty:
                    pass
                except asyncio.TimeoutError:
                    pass

                await flush_batch()

            # 종료 시 처리
            logger.info(f"Partition {partition_id} draining remaining messages...")
            await flush_batch()

            remaining_count = await drain_remaining_queue()
            if remaining_count > 0:
                logger.info(
                    f"Partition {partition_id} flushed {remaining_count} remaining messages"
                )

            logger.info(f"Partition {partition_id} worker exited cleanly")

        except asyncio.CancelledError:
            logger.warning(f"Partition {partition_id} worker cancelled")
            await flush_batch()

            remaining_count = await drain_remaining_queue()
            if remaining_count > 0:
                logger.warning(
                    f"Partition {partition_id} flushed {remaining_count} remaining messages on cancel"
                )
                self.otel_manager.batch_publisher_dropped_messages_counter.add(
                    remaining_count
                )

            raise
        except Exception as e:
            logger.error(f"Partition {partition_id} worker error: {e}", exc_info=True)
            await flush_batch()
            raise
        finally:
            logger.debug(f"Partition {partition_id} worker terminated")

    async def _flush(self, batch: list[tuple[str, bytes]], partition_id: int):
        """배치를 Redis에 publish"""
        total = len(batch)
        if total == 0:
            return

        with self.otel_manager.tracer.start_as_current_span(
            "batch_publisher.flush",
            attributes={
                "batch.message_count": total,
                "partition_id": partition_id,
            },
        ) as span:
            try:
                await self.redis_adapter.publish_batch(batch)
                span.set_attribute("flush.success", True)
                logger.debug(f"Flushed {total} messages from partition {partition_id}")
            except Exception as e:
                span.set_attribute("flush.success", False)
                span.record_exception(e)
                logger.error(
                    f"Publish failed (partition {partition_id}): {e}",
                    exc_info=True,
                    extra={"batch_size": total, "partition_id": partition_id},
                )

    async def add(self, message: dict, trace_context: dict | None = None):
        """
        메시지를 파티션 큐에 추가

        Raises:
            ServiceUnavailableError: 서비스 종료 중일 때
            BatchPublisherFullError: 큐가 가득 찼을 때
        """
        # 종료 중이면 메시지 거부
        if self._stop_event.is_set():
            logger.warning("BatchPublisher is stopping, rejecting message")
            raise ServiceUnavailableError("BatchPublisher is stopping")

        message["trace_context"] = trace_context
        partition_id = hash(message["user_id"]) % self.num_partitions

        try:
            channel, payload = self.redis_adapter.prepare_message(message)
            await asyncio.wait_for(
                self._partition_queues[partition_id].put((channel, payload)),
                timeout=self.queue_add_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Queue timeout, dropping message",
                extra={
                    "user_id": message["user_id"],
                    "room_id": message["room_id"],
                    "partition_id": partition_id,
                },
            )
            self.otel_manager.batch_publisher_dropped_messages_counter.add(1)
            raise BatchPublisherFullError(f"Queue Full (partition: {partition_id})")
        except Exception as e:
            logger.error(
                f"Failed to add message to partition {partition_id}: {e}",
                exc_info=True,
                extra={
                    "user_id": message.get("user_id"),
                    "room_id": message.get("room_id"),
                    "partition_id": partition_id,
                },
            )
            self.otel_manager.batch_publisher_dropped_messages_counter.add(1)
            raise
