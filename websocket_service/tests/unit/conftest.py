import pytest
from unittest.mock import AsyncMock, Mock

from src.application.broadcaster import Broadcaster


@pytest.fixture
def mock_redis_adapter():
    """Redis Adapter mock - unit 테스트용"""
    mock = AsyncMock()

    # 기본 동작 설정
    async def empty_listen():
        if False:
            yield

    mock.listen_pubsub = empty_listen
    mock.unsubscribe = AsyncMock()
    mock.publish = AsyncMock()
    return mock


@pytest.fixture
def mock_conn_manager():
    """Connection Manager mock - unit 테스트용"""
    mock = AsyncMock()
    mock.get_room_connections = AsyncMock(return_value=[])
    mock.disconnect = AsyncMock()
    return mock


@pytest.fixture
def broadcaster(mock_otel_manager, mock_redis_adapter, mock_conn_manager):
    """Broadcaster 인스턴스 - unit 테스트용"""
    return Broadcaster(
        otel_manager=mock_otel_manager,
        redis_adapter=mock_redis_adapter,
        conn_manager=mock_conn_manager,
        max_queue_size=10,
        num_message_dispatchers=2,
        cleanup_interval=0.1,
    )
