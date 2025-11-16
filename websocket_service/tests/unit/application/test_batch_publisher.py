import asyncio
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from src.application.exceptions import (
    BatchPublisherFullError,
    ServiceUnavailableError,
)
from src.application.batch_publisher import BatchPublisher


@pytest.fixture
def mock_redis_adapter():
    mock = AsyncMock()
    mock.prepare_message = lambda msg: (msg["room_id"], str(msg).encode())
    mock.publish_batch = AsyncMock()
    return mock


@pytest.fixture
def batch_publisher(mock_otel_manager, mock_redis_adapter):
    batch_publisher = BatchPublisher(
        otel_manager=mock_otel_manager,
        redis_adapter=mock_redis_adapter,
        flush_interval=0.05,  # 테스트를 위해 짧게 설정
        max_batch_size=3,
        num_partitions=2,
        max_queue_size=10,
    )
    return batch_publisher


@pytest.mark.asyncio
class TestBatchPublisher:
    """BatchPublisher 테스트"""

    @pytest_asyncio.fixture(autouse=True)
    async def test_start_and_stop(self, batch_publisher):
        """시작 및 정상 종료"""
        await batch_publisher.start()
        assert len(batch_publisher._partition_tasks) == 2
        await batch_publisher.stop()
        for task in batch_publisher._partition_tasks:
            assert task.done()

    @pytest_asyncio.fixture(autouse=True)
    async def test_add_and_flush_by_batch_size(
        self, batch_publisher, mock_redis_adapter
    ):
        """배치 크기 도달 시 flush"""
        await batch_publisher.start()

        messages = [
            {"room_id": "room1", "user_id": "user1", "message": "1"},
            {"room_id": "room1", "user_id": "user3", "message": "2"},  # 다른 파티션
            {"room_id": "room1", "user_id": "user1", "message": "3"},
        ]

        for msg in messages:
            await batch_publisher.add(msg)

        await asyncio.sleep(0.1)  # flush 대기

        # user1, user3은 다른 파티션으로 갈 수 있음
        assert mock_redis_adapter.publish_batch.call_count >= 1

        await batch_publisher.stop()

    @pytest_asyncio.fixture(autouse=True)
    async def test_add_and_flush_by_interval(self, batch_publisher, mock_redis_adapter):
        """flush 간격 도달 시 flush"""
        await batch_publisher.start()

        await batch_publisher.add(
            {"room_id": "room1", "user_id": "user1", "message": "1"}
        )

        await asyncio.sleep(0.1)  # flush_interval 보다 길게

        mock_redis_adapter.publish_batch.assert_called_once()
        await batch_publisher.stop()

    @pytest_asyncio.fixture(autouse=True)
    async def test_stop_flushes_remaining(self, batch_publisher, mock_redis_adapter):
        """stop() 호출 시 남은 메시지 flush"""
        await batch_publisher.start()

        await batch_publisher.add(
            {"room_id": "room1", "user_id": "user1", "message": "1"}
        )
        await batch_publisher.add(
            {"room_id": "room1", "user_id": "user1", "message": "2"}
        )

        await batch_publisher.stop()

        mock_redis_adapter.publish_batch.assert_called_once()

    @pytest_asyncio.fixture(autouse=True)
    async def test_batch_publisher_full(self, batch_publisher, mock_redis_adapter):
        """버퍼가 가득 찼을 때 예외 발생"""
        batch_publisher.max_queue_size = 1
        batch_publisher.queue_add_timeout = 0.01

        # 워커를 제어하기 위한 이벤트
        flush_started = asyncio.Event()
        flush_can_continue = asyncio.Event()

        async def slow_flush(*args, **kwargs):
            flush_started.set()
            await flush_can_continue.wait()

        # flush를 느리게 만들어 워커를 제어
        mock_redis_adapter.publish_batch.side_effect = slow_flush

        await batch_publisher.start()

        # user_id를 고정하여 같은 파티션으로 메시지를 보냄
        user_id = "user1"

        # 첫 메시지를 보내 워커가 flush에서 대기하도록 함
        await batch_publisher.add(
            {"room_id": "room1", "user_id": user_id, "message": "first"}
        )

        # 워커가 flush를 시작할 때까지 대기
        await flush_started.wait()

        # 이제 워커는 slow_flush에서 대기 중이고, 큐는 비어있음
        # 큐를 가득 채움 (max_queue_size = 1)
        await batch_publisher.add(
            {"room_id": "room1", "user_id": user_id, "message": "second"}
        )

        # 큐가 가득 찼으므로 예외 발생을 기대
        with pytest.raises(BatchPublisherFullError):
            await batch_publisher.add(
                {"room_id": "room1", "user_id": user_id, "message": "third"}
            )

        # 워커가 계속 진행하도록 허용
        flush_can_continue.set()
        await batch_publisher.stop()

    @pytest_asyncio.fixture(autouse=True)
    async def test_add_to_stopping_batch_publisher(self, batch_publisher):
        """종료 중인 버퍼에 추가 시 예외 발생"""
        await batch_publisher.start()
        await batch_publisher.stop()

        with pytest.raises(ServiceUnavailableError):
            await batch_publisher.add(
                {"room_id": "room1", "user_id": "user1", "message": "1"}
            )
