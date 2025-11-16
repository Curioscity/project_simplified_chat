import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, call

from src.application.storage_processor import MessageStorageProcessor
from src.infrastructure.kafka import KafkaMessageConsumer, KafkaProducerAdapter
from src.infrastructure.message_repository import MessageRepository
from src.infrastructure.otel import OTELManager


@pytest.fixture
def mock_otel_manager():
    """Fixture for a mock OTELManager."""
    mock = MagicMock(spec=OTELManager)
    mock.tracer = MagicMock()
    mock.tracer.start_as_current_span.return_value.__enter__.return_value = MagicMock()
    mock.tracer.get_current_span.return_value = None
    mock.dropped_messages_counter = MagicMock()
    return mock


@pytest.fixture
def mock_kafka_consumer():
    """Fixture for a mock KafkaMessageConsumer."""
    mock = AsyncMock(spec=KafkaMessageConsumer)
    return mock


@pytest.fixture
def mock_message_repository():
    """Fixture for a mock MessageRepository."""
    mock = AsyncMock(spec=MessageRepository)
    # 수정: AsyncMock 중복 래핑 제거
    mock.save_messages.return_value = {"inserted": 1, "failed": [], "duplicates": 0}
    return mock


@pytest.fixture
def mock_kafka_producer():
    """Fixture for a mock KafkaProducerAdapter."""
    mock = AsyncMock(spec=KafkaProducerAdapter)
    return mock


@pytest.fixture
def storage_processor(
    mock_otel_manager,
    mock_kafka_consumer,
    mock_message_repository,
    mock_kafka_producer,
):
    """Fixture for a MessageStorageProcessor instance with mocked dependencies."""
    return MessageStorageProcessor(
        otel_manager=mock_otel_manager,
        kafka_message_consumer=mock_kafka_consumer,
        message_repository=mock_message_repository,
        kafka_producer_adapter=mock_kafka_producer,
        max_batch_size=2,
        flush_interval=0.05,
        shutdown_timeout=1.0,
    )


# === 기존 테스트 개선 ===


@pytest.mark.asyncio
async def test_storage_processor_start_stop(storage_processor):
    """Test that the storage processor starts and stops correctly."""
    await storage_processor.start()
    assert storage_processor._listener_task is not None
    assert storage_processor._flush_task is not None
    assert not storage_processor._stop_event.is_set()

    await storage_processor.stop()
    assert storage_processor._stop_event.is_set()
    assert storage_processor._listener_task.done()
    assert storage_processor._flush_task.done()


@pytest.mark.asyncio
async def test_listener_worker_enqueues_messages(
    storage_processor, mock_kafka_consumer
):
    """Test that the listener worker correctly enqueues messages from Kafka."""
    test_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
        {"room_id": "r2", "user_id": "u2", "content": "msg2", "timestamp": "t2"},
    ]
    messages_yielded = asyncio.Event()

    async def async_message_generator():
        for msg in test_messages:
            yield msg
        messages_yielded.set()

    mock_kafka_consumer.listen.return_value = async_message_generator()

    await storage_processor.start()
    # Isolate the listener by cancelling the flush task
    storage_processor._flush_task.cancel()

    try:
        await asyncio.wait_for(messages_yielded.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Timeout waiting for messages to be yielded")

    # Give the listener a moment to process the last message
    await asyncio.sleep(0.1)

    assert storage_processor._batch_queue.qsize() == len(test_messages)

    enqueued = []
    while not storage_processor._batch_queue.empty():
        enqueued.append(storage_processor._batch_queue.get_nowait())

    assert sorted(enqueued, key=lambda x: x["timestamp"]) == sorted(
        test_messages, key=lambda x: x["timestamp"]
    )
    await storage_processor.stop()


@pytest.mark.asyncio
async def test_flush_worker_processes_batch_when_max_size_reached(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that flush worker immediately processes when batch reaches max_batch_size."""
    test_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
        {"room_id": "r2", "user_id": "u2", "content": "msg2", "timestamp": "t2"},
    ]

    for msg in test_messages:
        await storage_processor._batch_queue.put(msg)

    await storage_processor.start()

    # 배치 크기(2)에 도달했으므로 flush_interval 전에 처리됨
    await asyncio.sleep(0.02)  # flush_interval(0.05)보다 짧게
    await storage_processor.stop()

    # save_messages가 호출되었는지 확인
    assert mock_message_repository.save_messages.call_count >= 1

    # 모든 호출에서 처리된 메시지 수집
    all_called_messages = []
    for call_args in mock_message_repository.save_messages.call_args_list:
        all_called_messages.extend(call_args[0][0])

    assert len(all_called_messages) == len(test_messages)
    assert all(msg in all_called_messages for msg in test_messages)
    mock_kafka_consumer.commit.assert_called()


@pytest.mark.asyncio
async def test_flush_batch_saves_and_commits(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that _flush_batch saves messages and commits Kafka offset on success."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
    ]

    await storage_processor._flush_batch(batch_messages)

    mock_message_repository.save_messages.assert_called_once_with(batch_messages)
    mock_kafka_consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_flush_batch_handles_failed_messages(
    storage_processor, mock_message_repository, mock_kafka_producer, mock_kafka_consumer
):
    """Test that _flush_batch sends failed messages to DLQ."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
    ]
    failed_message = batch_messages[0]
    mock_message_repository.save_messages.return_value = {
        "failed_messages": [failed_message],
        "inserted": 0,
        "duplicates": 0,
    }

    await storage_processor._flush_batch(batch_messages)

    mock_kafka_producer.send_chat_message_to_dlq.assert_called_once_with(
        failed_message, error_reason="mongodb_write_failed"
    )
    mock_message_repository.save_messages.assert_called_once_with(batch_messages)
    # failed_messages만 있고 inserted/duplicates가 없으면 commit 안 함
    mock_kafka_consumer.commit.assert_not_called()


@pytest.mark.asyncio
async def test_flush_batch_no_commit_on_no_processed_messages(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that _flush_batch does not commit if no messages were inserted or duplicated."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
    ]
    mock_message_repository.save_messages.return_value = {
        "inserted": 0,
        "failed": [],
        "duplicates": 0,
    }

    await storage_processor._flush_batch(batch_messages)

    mock_message_repository.save_messages.assert_called_once_with(batch_messages)
    mock_kafka_consumer.commit.assert_not_called()


# === 새로운 테스트 케이스 ===


@pytest.mark.asyncio
async def test_listener_handles_missing_required_field(
    storage_processor, mock_kafka_consumer
):
    """Test that listener logs error when message is missing required field."""
    invalid_message = {"room_id": "r1", "user_id": "u1"}  # message, timestamp 누락

    async def async_message_generator():
        yield invalid_message

    mock_kafka_consumer.listen.return_value = async_message_generator()

    await storage_processor.start()

    try:
        await asyncio.wait_for(storage_processor._listener_task, timeout=1.0)
    except asyncio.CancelledError:
        pass

    # 메시지가 큐에 추가되지 않아야 함
    await asyncio.sleep(0.05)
    assert storage_processor._batch_queue.qsize() == 0
    await storage_processor.stop()


@pytest.mark.asyncio
async def test_graceful_shutdown_flushes_remaining_messages(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that graceful shutdown flushes all remaining messages in queue."""
    test_messages = [
        {
            "room_id": f"r{i}",
            "user_id": f"u{i}",
            "content": f"msg{i}",
            "timestamp": f"t{i}",
        }
        for i in range(5)
    ]

    for msg in test_messages:
        await storage_processor._batch_queue.put(msg)

    await storage_processor.start()
    await asyncio.sleep(0.01)  # 일부 처리 허용
    await storage_processor.stop()

    # 모든 메시지가 처리되었는지 확인
    all_saved = []
    for call_args in mock_message_repository.save_messages.call_args_list:
        all_saved.extend(call_args[0][0])

    assert len(all_saved) == len(test_messages)
    assert all(msg in all_saved for msg in test_messages)


@pytest.mark.asyncio
async def test_flush_batch_handles_duplicates(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that _flush_batch handles duplicate messages correctly."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
    ]
    mock_message_repository.save_messages.return_value = {
        "inserted": 0,
        "duplicates": 1,
        "failed": [],
    }

    await storage_processor._flush_batch(batch_messages)

    mock_message_repository.save_messages.assert_called_once_with(batch_messages)
    # duplicates도 처리된 것으로 간주하여 commit
    mock_kafka_consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_flush_batch_handles_repository_exception(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that _flush_batch handles repository exceptions gracefully."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
    ]
    mock_message_repository.save_messages.side_effect = Exception(
        "DB connection failed"
    )

    # 예외가 발생해도 전체 프로세스가 중단되지 않아야 함
    await storage_processor._flush_batch(batch_messages)

    mock_message_repository.save_messages.assert_called_once_with(batch_messages)
    # 예외 발생 시 commit 안 함
    mock_kafka_consumer.commit.assert_not_called()


@pytest.mark.asyncio
async def test_flush_interval_triggers_batch_processing(
    storage_processor, mock_message_repository, mock_kafka_consumer
):
    """Test that flush_interval triggers batch processing even with small batch."""
    test_message = {
        "room_id": "r1",
        "user_id": "u1",
        "content": "msg1",
        "timestamp": "t1",
    }

    await storage_processor._batch_queue.put(test_message)
    await storage_processor.start()

    # flush_interval만큼 대기 (배치 크기 미달)
    await asyncio.sleep(storage_processor.flush_interval + 0.02)
    await storage_processor.stop()

    # flush_interval에 의해 처리되었는지 확인
    mock_message_repository.save_messages.assert_called()

    # 호출된 모든 배치에서 메시지 찾기
    found = False
    for call_args in mock_message_repository.save_messages.call_args_list:
        batch = call_args[0][0]
        if test_message in batch:
            found = True
            break

    assert found, "Message should be processed by flush interval"


@pytest.mark.asyncio
async def test_flush_batch_with_partial_success(
    storage_processor, mock_message_repository, mock_kafka_producer, mock_kafka_consumer
):
    """Test that _flush_batch handles partial success (some inserted, some failed)."""
    batch_messages = [
        {"room_id": "r1", "user_id": "u1", "content": "msg1", "timestamp": "t1"},
        {"room_id": "r2", "user_id": "u2", "content": "msg2", "timestamp": "t2"},
    ]

    mock_message_repository.save_messages.return_value = {
        "inserted": 1,
        "duplicates": 0,
        "failed_messages": [batch_messages[1]],
    }

    await storage_processor._flush_batch(batch_messages)

    # DLQ로 실패한 메시지만 전송
    mock_kafka_producer.send_chat_message_to_dlq.assert_called_once_with(
        batch_messages[1], error_reason="mongodb_write_failed"
    )

    # inserted가 있으므로 commit
    mock_kafka_consumer.commit.assert_called_once()


@pytest.mark.asyncio
async def test_stop_idempotency(storage_processor):
    """Test that calling stop multiple times is safe."""
    await storage_processor.start()

    # 첫 번째 stop
    await storage_processor.stop()
    assert storage_processor._stop_event.is_set()

    # 두 번째 stop (이미 중지됨)
    await storage_processor.stop()
    assert storage_processor._stop_event.is_set()


@pytest.mark.asyncio
async def test_empty_batch_flush_is_noop(storage_processor, mock_message_repository):
    """Test that flushing an empty batch does nothing."""
    await storage_processor._flush_batch([])

    # 빈 배치는 처리하지 않음
    mock_message_repository.save_messages.assert_not_called()
