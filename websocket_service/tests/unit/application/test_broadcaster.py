import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from orjson import orjson

from src.application.broadcaster import Broadcaster


@pytest.fixture
def mock_otel_manager():
    """Fixture for a mocked OTELManager."""
    manager = MagicMock()
    manager.tracer.start_as_current_span.return_value.__enter__.return_value = None
    return manager


@pytest.fixture
def mock_redis_adapter():
    """Fixture for a mocked RedisAdapter."""
    return AsyncMock()


@pytest.fixture
def mock_conn_manager():
    """Fixture for a mocked ConnectionManager."""
    return AsyncMock()


@pytest.fixture
def broadcaster(mock_otel_manager, mock_redis_adapter, mock_conn_manager):
    """Fixture to create a Broadcaster instance for testing."""
    # Use smaller, test-friendly values
    return Broadcaster(
        otel_manager=mock_otel_manager,
        redis_adapter=mock_redis_adapter,
        conn_manager=mock_conn_manager,
        cleanup_interval=0.1,
        num_message_dispatchers=2,
    )


@pytest.mark.asyncio
class TestBroadcasterLifecycle:
    """Tests the start, stop, and lifecycle management of the broadcaster."""

    async def test_start_creates_background_tasks(self, broadcaster):
        """Verify that start() creates all necessary background tasks."""
        with patch("asyncio.create_task") as mock_create_task:
            await broadcaster.start()

            # 1 for _pubsub_listener_worker
            # 1 for _room_cleanup_worker
            # num_message_dispatchers for _message_dispatcher_worker
            expected_task_calls = 1 + 1 + broadcaster.num_message_dispatchers
            assert mock_create_task.call_count == expected_task_calls

            # Stop to clean up tasks
            await broadcaster.stop()

    async def test_stop_gracefully_shuts_down_tasks(self, broadcaster):
        """Verify that stop() cancels and cleans up all running tasks."""
        await broadcaster.start()

        # Ensure tasks are running
        assert broadcaster._pubsub_listener_task is not None
        assert broadcaster._room_cleanup_task is not None
        assert len(broadcaster._dispatcher_tasks) == broadcaster.num_message_dispatchers

        # Spy on the tasks to check for cancellation
        original_cancel = asyncio.Task.cancel
        cancel_mocks = {}

        def spy_cancel(task, name):
            if name not in cancel_mocks:
                cancel_mocks[name] = MagicMock()

            cancel_mocks[name]()
            return original_cancel(task)

        broadcaster._pubsub_listener_task.cancel = lambda: spy_cancel(
            broadcaster._pubsub_listener_task, "pubsub"
        )
        broadcaster._room_cleanup_task.cancel = lambda: spy_cancel(
            broadcaster._room_cleanup_task, "cleanup"
        )
        for i, task in enumerate(broadcaster._dispatcher_tasks):
            task.cancel = lambda t=task, index=i: spy_cancel(t, f"dispatcher_{index}")

        await broadcaster.stop()

        # Check that tasks were cancelled
        assert "pubsub" in cancel_mocks
        assert "cleanup" in cancel_mocks

        # Check that all tasks are actually done
        assert broadcaster._pubsub_listener_task.done()
        assert broadcaster._room_cleanup_task.done()
        assert all(t.done() for t in broadcaster._dispatcher_tasks)
        assert not broadcaster._room_tasks  # Should be cleared


@pytest.mark.asyncio
class TestMessageFlow:
    """Tests the end-to-end flow of a message from Redis to WebSocket clients."""

    async def test_message_from_pubsub_is_broadcast_to_room(
        self, broadcaster, mock_redis_adapter, mock_conn_manager
    ):
        """
        Tests the full message pipeline:
        1. Message received from Redis Pub/Sub.
        2. Dispatched to the correct room.
        3. Broadcast to all connections in that room.
        """
        room_id = "test-room-1"
        message_content = {"room_id": room_id, "data": "hello world"}

        # 1. Setup mock Redis message
        redis_message = {"type": "message", "data": orjson.dumps(message_content)}

        # Use an asyncio.Queue to simulate the async iterator from redis
        redis_queue = asyncio.Queue()
        redis_queue.put_nowait(redis_message)

        async def redis_listen_generator():
            msg = await redis_queue.get()
            yield msg
            # Keep the listener alive until the test is done
            await asyncio.sleep(5)

        mock_redis_adapter.listen_pubsub = redis_listen_generator

        # 2. Setup mock connections for the room
        mock_conn1 = AsyncMock()
        mock_conn1.send = AsyncMock(return_value=True)
        mock_conn2 = AsyncMock()
        mock_conn2.send = AsyncMock(return_value=True)
        mock_conn_manager.get_room_connections.return_value = [mock_conn1, mock_conn2]

        # 3. Start the broadcaster and let it process the message
        await broadcaster.start()

        # Allow time for the message to go through the pubsub listener,
        # dispatcher, and room broadcaster worker.
        await asyncio.sleep(0.1)

        # 4. Assert that the message was sent to all connections
        mock_conn_manager.get_room_connections.assert_called_with(room_id)

        expected_payload = orjson.dumps(message_content)
        mock_conn1.send.assert_awaited_once_with(expected_payload)
        mock_conn2.send.assert_awaited_once_with(expected_payload)

        await broadcaster.stop()


@pytest.mark.asyncio
class TestRoomManagement:
    """Tests the creation and cleanup of room-specific resources."""

    async def test_room_worker_created_on_first_message(self, broadcaster):
        """Verify a new room queue and worker are created for a new room_id."""
        room_id = "new-room"

        assert room_id not in broadcaster._room_queues
        assert room_id not in broadcaster._room_tasks

        # Directly call the internal method that creates rooms.
        # This is a focused unit test, avoiding the complexity of the full E2E flow.
        await broadcaster._get_or_create_room_queue(room_id)

        assert room_id in broadcaster._room_queues
        assert isinstance(broadcaster._room_queues[room_id], asyncio.Queue)
        assert room_id in broadcaster._room_tasks
        assert isinstance(broadcaster._room_tasks[room_id], asyncio.Task)

        # Clean up the created task
        broadcaster._room_tasks[room_id].cancel()
        try:
            await broadcaster._room_tasks[room_id]
        except asyncio.CancelledError:
            pass

    async def test_empty_room_is_cleaned_up(self, broadcaster, mock_conn_manager):
        """Verify that a room with no connections and an empty queue is removed."""
        room_id = "empty-room"

        # 1. Manually create the state for an existing, idle room
        queue = asyncio.Queue()
        task = asyncio.create_task(broadcaster._room_worker(room_id, queue))
        broadcaster._room_queues[room_id] = queue
        broadcaster._room_tasks[room_id] = task

        # 2. Setup the condition for cleanup: no connections in the room
        mock_conn_manager.get_room_connections.return_value = []

        # 3. Directly call the cleanup logic for a focused test
        cleaned = await broadcaster._cleanup_empty_room(room_id)

        # 4. Assert that the room's resources were removed
        assert cleaned is True
        assert room_id not in broadcaster._room_queues
        assert room_id not in broadcaster._room_tasks
        broadcaster.redis_adapter.unsubscribe.assert_awaited_once_with(room_id)

        # The task should be cancelled by the cleanup logic
        assert task.done()
