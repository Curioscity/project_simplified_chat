import asyncio
import logging
import random
import time

from opentelemetry import propagate
from orjson import orjson

from src.application.connection_manager import ConnectionManager, Connection
from src.common.websocket_close_code import WebSocketCloseCode
from src.infrastructure.otel import OTELManager
from src.infrastructure.redis import RedisAdapter

logger = logging.getLogger(__name__)


class Broadcaster:
    """Redis Pub/Sub을 통한 메시지 브로드캐스팅"""

    def __init__(
        self,
        otel_manager: OTELManager,
        redis_adapter: RedisAdapter,
        conn_manager: ConnectionManager,
        max_queue_size: int = 1_000,
        queue_put_timeout: float = 0.1,
        cleanup_interval: float = 30.0,
        num_locks: int = 1000,
        num_message_dispatchers: int = 16,
    ):
        self.otel_manager = otel_manager
        self.redis_adapter = redis_adapter
        self.conn_manager = conn_manager
        self.max_queue_size = max_queue_size
        self.queue_put_timeout = queue_put_timeout
        self.cleanup_interval = cleanup_interval
        self.num_message_dispatchers = num_message_dispatchers

        # 방별 큐와 태스크
        self._room_queues: dict[str, asyncio.Queue] = {}
        self._room_tasks: dict[str, asyncio.Task] = {}

        # 락 풀링
        self._locks = [asyncio.Lock() for _ in range(num_locks)]

        # 메시지 디스패처 큐와 태스크
        self._dispatcher_queues: list[asyncio.Queue] = []
        self._dispatcher_tasks: list[asyncio.Task] = []

        # 백그라운드 태스크
        self._pubsub_listener_task: asyncio.Task | None = None
        self._room_cleanup_task: asyncio.Task | None = None

        self._stop_event = asyncio.Event()

    async def start(self):
        """브로드캐스터 시작"""
        if self._pubsub_listener_task:
            logger.warning("Already running")
            return

        for i in range(self.num_message_dispatchers):
            queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._dispatcher_queues.append(queue)
            task = asyncio.create_task(self._message_dispatcher_worker(i, queue))
            self._dispatcher_tasks.append(task)

        self._pubsub_listener_task = asyncio.create_task(self._pubsub_listener_worker())
        self._room_cleanup_task = asyncio.create_task(self._room_cleanup_worker())
        logger.info("Broadcaster started")

    async def stop(self):
        """브로드캐스터 종료 - 모든 큐를 완전히 비움"""
        logger.info("Broadcaster stopping...")
        self._stop_event.set()

        # 1. pubsub_listener_task 및 room_cleanup_task 즉시 종료
        tasks_to_cancel = []
        if self._pubsub_listener_task and not self._pubsub_listener_task.done():
            tasks_to_cancel.append(self._pubsub_listener_task)
        if self._room_cleanup_task and not self._room_cleanup_task.done():
            tasks_to_cancel.append(self._room_cleanup_task)

        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        # 2. 메시지 디스패처들이 큐를 비울 때까지 대기
        if self._dispatcher_tasks:
            logger.info(
                f"Waiting for {len(self._dispatcher_tasks)} message dispatchers to finish..."
            )

            done, pending = await asyncio.wait(
                self._dispatcher_tasks,
                timeout=5.0,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                total_remaining = sum(q.qsize() for q in self._dispatcher_queues)
                logger.warning(
                    f"{len(pending)} message dispatchers still running, "
                    f"{total_remaining} messages remaining"
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

        # 3. 방 워커 종료 대기
        if self._room_tasks:
            done, pending = await asyncio.wait(
                self._room_tasks.values(),
                timeout=3.0,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                logger.warning(
                    f"{len(pending)} room workers still running, forcing shutdown"
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*self._room_tasks.values(), return_exceptions=True)

        # 5. Redis 구독 해제
        unsub_tasks = [
            self.redis_adapter.unsubscribe(room_id)
            for room_id in list(self._room_queues.keys())
        ]
        if unsub_tasks:
            await asyncio.gather(*unsub_tasks, return_exceptions=True)

        # 6. 리소스 정리
        self._room_queues.clear()
        self._room_tasks.clear()
        self._dispatcher_queues.clear()
        self._dispatcher_tasks.clear()

        logger.info("Broadcaster stopped")

    async def _pubsub_listener_worker(self):
        """Redis Pub/Sub 메시지 수신"""
        retry_count = 0
        max_retries = 5
        base_delay = 1

        while not self._stop_event.is_set():
            try:
                async for message in self.redis_adapter.listen_pubsub():
                    if self._stop_event.is_set():
                        logger.info("Stop event set, exiting Redis listener")
                        break

                    if message["type"] != "message":
                        continue

                    retry_count = 0

                    # worker_id = random.randint(0, self.num_message_dispatchers - 1)  단순 라운드 로빈용
                    worker_id = (
                        hash(message["channel"]) % self.num_message_dispatchers
                    )  # 순서 보장용
                    try:
                        self._dispatcher_queues[worker_id].put_nowait(message)
                    except asyncio.QueueFull:
                        logger.warning(
                            f"Message dispatcher queue {worker_id} full, dropping message"
                        )
                        self.otel_manager.broadcaster_dropped_messages_counter.add(1)

            except asyncio.CancelledError:
                logger.info("Redis listener cancelled")
                raise

            except Exception as e:
                retry_count += 1
                delay = min(base_delay * (2**retry_count), 60)
                logger.error(
                    f"Redis error (attempt {retry_count}/{max_retries}): {e}",
                    exc_info=True,
                )
                if retry_count < max_retries:
                    await asyncio.sleep(delay)
                else:
                    logger.critical("Redis listener stopped. Max retries reached")
                    raise

    async def _message_dispatcher_worker(self, worker_id: int, queue: asyncio.Queue):
        """메시지 디스패처 워커 - 독립 큐 처리"""
        logger.info(f"Message dispatcher worker {worker_id} started")

        try:
            while True:
                if self._stop_event.is_set() and queue.empty():
                    logger.info(
                        f"Message dispatcher worker {worker_id} exiting cleanly"
                    )
                    break

                try:
                    message = await asyncio.wait_for(queue.get(), timeout=1.0)

                    try:
                        await self._dispatch_to_room_queue(message)
                    except Exception as e:
                        logger.error(
                            f"Worker {worker_id} error dispatching message: {e}",
                            exc_info=True,
                        )
                    finally:
                        queue.task_done()

                except asyncio.TimeoutError:
                    continue

        except asyncio.CancelledError:
            logger.info(f"Message dispatcher worker {worker_id} cancelled")

            # 취소되어도 남은 메시지 처리
            remaining = 0
            while not queue.empty():
                try:
                    message = queue.get_nowait()
                    remaining += 1
                    await self._dispatch_to_room_queue(message)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"Error processing remaining message: {e}")

            if remaining > 0:
                logger.info(
                    f"Worker {worker_id} processed {remaining} remaining messages"
                )

            raise
        finally:
            logger.debug(f"Message dispatcher worker {worker_id} terminated")

    async def _dispatch_to_room_queue(self, message: dict):
        """Redis 메시지를 파싱하여 방 큐에 추가"""
        try:
            message_data = orjson.loads(message["data"])
            room_id = message_data["room_id"]

            queue = await self._get_or_create_room_queue(room_id)

            try:
                await asyncio.wait_for(
                    queue.put(message_data), timeout=self.queue_put_timeout
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"Queue full for room {room_id}, dropping message",
                    extra={"room_id": room_id},
                )
                self.otel_manager.broadcaster_dropped_messages_counter.add(1)

        except orjson.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error dispatching message: {e}", exc_info=True)

    def _get_room_lock(self, room_id: str) -> asyncio.Lock:
        """room_id를 해싱하여 락 반환"""
        index = hash(room_id) % len(self._locks)
        return self._locks[index]

    async def _get_or_create_room_queue(self, room_id: str) -> asyncio.Queue:
        """방 큐 조회 또는 생성"""
        # Fast path
        if queue := self._room_queues.get(room_id):
            if task := self._room_tasks.get(room_id):
                if not task.done():
                    return queue

        # Slow path
        lock = self._get_room_lock(room_id)
        async with lock:
            if queue := self._room_queues.get(room_id):
                if task := self._room_tasks.get(room_id):
                    if not task.done():
                        return queue

            queue = asyncio.Queue(maxsize=self.max_queue_size)
            task = asyncio.create_task(self._room_worker(room_id, queue))

            self._room_queues[room_id] = queue
            self._room_tasks[room_id] = task

            logger.info(f"Created queue and worker for room: {room_id}")
            return queue

    async def _room_worker(self, room_id: str, queue: asyncio.Queue):
        """방별 워커 - 큐에서 메시지를 가져와 브로드캐스트"""
        logger.debug(f"Room broadcaster worker for {room_id} started")

        try:
            while True:
                if self._stop_event.is_set() and queue.empty():
                    logger.debug(f"Room broadcaster worker {room_id} exiting cleanly")
                    break

                try:
                    message_data = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                self.otel_manager.broadcaster_room_queue_size_histogram.record(
                    queue.qsize(), attributes={"room_id": room_id}
                )

                try:
                    await self._broadcast_to_connections(
                        room_id, message_data, message_data.get("trace_context")
                    )
                except Exception as e:
                    logger.error(
                        f"Broadcast error in room {room_id}: {e}", exc_info=True
                    )
                finally:
                    queue.task_done()

        except asyncio.CancelledError:
            logger.info(f"Room broadcaster worker for {room_id} cancelled")

            # 잔여 메시지 처리
            remaining = 0
            while True:
                try:
                    message = queue.get_nowait()
                    remaining += 1
                    await self._broadcast_to_connections(
                        room_id, message, message.get("trace_context")
                    )
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"Error draining queue for {room_id}: {e}")

            if remaining > 0:
                logger.info(f"Drained {remaining} messages from room {room_id}")

            raise
        finally:
            logger.debug(f"Room broadcaster worker for {room_id} terminated")

    async def _broadcast_to_connections(
        self, room_id: str, message_data: dict, trace_context: dict | None = None
    ):
        """방의 모든 연결에 병렬로 메시지 브로드캐스트"""
        connections = await self.conn_manager.get_room_connections(room_id)
        if not connections:
            return

        # Observability
        extracted_context = None
        if trace_context:
            try:
                extracted_context = propagate.extract(trace_context)
            except Exception:
                pass

        with self.otel_manager.tracer.start_as_current_span(
            "websocket.broadcast",
            context=extracted_context,
            attributes={
                "room_id": room_id,
                "user_id": message_data.get("user_id"),
                "connection_count": len(connections),
            },
        ) as span:
            message_bytes = orjson.dumps(message_data)

            send_tasks = {
                connection: asyncio.create_task(connection.send(message_bytes))
                for connection in connections
            }

            asyncio.create_task(self._collect_and_cleanup_connections(send_tasks))

            span.set_attribute("broadcast.completed", True)

            if e2e_start := message_data.get("_e2e_start"):
                latency_ms = (time.monotonic() - e2e_start) * 1000
                self.otel_manager.broadcast_latency_histogram.record(latency_ms)

    async def _collect_and_cleanup_connections(
        self, send_tasks: dict[Connection, asyncio.Task]
    ) -> None:
        """백그라운드에서 결과 수집 후 실패한 것만 정리"""
        try:
            results = await asyncio.gather(*send_tasks.values(), return_exceptions=True)

            failed_connections = []
            for (connection, task), result in zip(send_tasks.items(), results):
                should_close, _ = connection.should_close()
                if isinstance(result, Exception) or result is False or should_close:
                    failed_connections.append(connection)

            if failed_connections:
                logger.info(
                    f"Cleaning up {len(failed_connections)} failed connections",
                    extra={"failed_count": len(failed_connections)},
                )

                cleanup_tasks = [
                    self.conn_manager.disconnect(
                        conn, WebSocketCloseCode.TRY_AGAIN_LATER
                    )
                    for conn in failed_connections
                ]

                cleanup_results = await asyncio.gather(
                    *cleanup_tasks, return_exceptions=True
                )

                errors = [r for r in cleanup_results if isinstance(r, Exception)]
                if errors:
                    logger.warning(f"Failed to cleanup {len(errors)} connections")

        except Exception as e:
            logger.error(f"Error in collect_and_cleanup for room: {e}", exc_info=True)

    async def _room_cleanup_worker(self):
        """주기적으로 빈 방 정리"""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_empty_rooms()
        except asyncio.CancelledError:
            logger.info("Room cleanup worker cancelled")
            raise
        except Exception as e:
            logger.error(f"Room cleanup worker error: {e}", exc_info=True)

    async def _cleanup_empty_rooms(self):
        """빈 방들을 병렬로 정리"""
        room_ids = list(self._room_queues.keys())
        if not room_ids:
            return

        logger.debug(f"Checking {len(room_ids)} rooms for cleanup")

        results = await asyncio.gather(
            *[self._cleanup_empty_room(room_id) for room_id in room_ids],
            return_exceptions=True,
        )

        cleaned = sum(1 for r in results if r is True)
        errors = sum(1 for r in results if isinstance(r, Exception))

        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} empty rooms")
        if errors > 0:
            logger.warning(f"Failed to clean up {errors} rooms")

    async def _cleanup_empty_room(self, room_id: str) -> bool:
        """빈 방 정리"""
        if room_id not in self._room_queues:
            return False

        try:
            lock = self._get_room_lock(room_id)
            async with lock:
                if room_id not in self._room_queues:
                    return False

                queue = self._room_queues[room_id]
                if queue.qsize() > 0:
                    return False

                connections = await self.conn_manager.get_room_connections(room_id)
                if connections:
                    return False

                # 워커 종료
                task = self._room_tasks.get(room_id)
                if task and not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=1.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

                self._room_queues.pop(room_id, None)
                self._room_tasks.pop(room_id, None)

            await self.redis_adapter.unsubscribe(room_id)

            logger.info(f"Cleaned up room: {room_id}")
            return True

        except Exception as e:
            logger.error(f"Error cleaning up room {room_id}: {e}", exc_info=True)
            return False
