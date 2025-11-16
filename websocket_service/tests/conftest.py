import pytest
import asyncio
from unittest.mock import Mock, MagicMock

import pytest_asyncio


# 이벤트 루프 설정
@pytest.fixture(scope="function")
def event_loop():
    """각 테스트마다 새 이벤트 루프"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# 공통 Mock fixtures
@pytest.fixture
def mock_otel_manager():
    """OTEL Manager mock - 모든 테스트에서 사용"""
    mock = Mock()
    mock.tracer = Mock()
    mock.tracer.start_as_current_span = MagicMock()
    mock.dropped_messages_counter = Mock()
    mock.broadcast_latency_histogram = Mock()
    return mock


@pytest_asyncio.fixture()
async def cleanup_tasks():
    """테스트 후 남은 태스크 정리 - 자동 적용"""
    yield

    tasks = [t for t in asyncio.all_tasks() if not t.done()]
    for task in tasks:
        task.cancel()

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
