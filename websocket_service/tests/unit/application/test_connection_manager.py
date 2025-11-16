import asyncio
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from src.application.connection_manager import (
    Connection,
    ConnectionManager,
)
from src.application.exceptions import ConnectionLimitExceeded
from src.common.websocket_close_code import WebSocketCloseCode


@pytest.fixture
def mock_websocket():
    """WebSocket mock"""
    ws = AsyncMock()
    ws.accept = AsyncMock()
    ws.send_bytes = AsyncMock(return_value=True)
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def connection(mock_websocket):
    """Connection 인스턴스"""
    return Connection(mock_websocket, "room1", "user1")


@pytest.fixture
def manager(mock_otel_manager):
    """ConnectionManager 인스턴스"""
    return ConnectionManager(otel_manager=mock_otel_manager, max_total_connections=10)


@pytest.mark.asyncio
class TestConnection:
    """Connection 클래스 테스트"""

    async def test_send_successful(self, connection, mock_websocket):
        """메시지 전송 성공"""
        result = await connection.send(b"test")
        assert result is True
        assert connection.consecutive_failures == 0
        mock_websocket.send_bytes.assert_called_once_with(b"test")

    async def test_send_timeout_and_recovery(self, connection, mock_websocket):
        """전송 타임아웃 후 복구"""
        mock_websocket.send_bytes.side_effect = asyncio.TimeoutError
        result = await connection.send(b"test")
        assert result is False
        assert connection.consecutive_failures == 1

        # 복구
        mock_websocket.send_bytes.side_effect = None
        result = await connection.send(b"test")
        assert result is True
        assert connection.consecutive_failures == 0

    async def test_send_too_many_failures_closes_connection(
        self, connection, mock_websocket
    ):
        """너무 많은 실패로 연결 종료"""
        connection.max_consecutive_failures = 2
        mock_websocket.send_bytes.side_effect = asyncio.TimeoutError

        await connection.send(b"test")
        await connection.send(b"test")

        assert connection._is_closed is True
        mock_websocket.close.assert_called_once()

    async def test_is_rate_limited(self, connection):
        """Rate limit 테스트"""
        connection.rate_limit_per_sec = 2
        assert connection.is_rate_limited() is False
        assert connection.is_rate_limited() is False
        assert connection.is_rate_limited() is True

        await asyncio.sleep(1.1)
        assert connection.is_rate_limited() is False


@pytest.mark.asyncio
class TestConnectionManager:
    """ConnectionManager 클래스 테스트"""

    async def test_connect_and_disconnect(self, manager, mock_websocket):
        """연결 및 연결 해제"""
        conn, is_first = await manager.connect("room1", "user1", mock_websocket)

        assert is_first is True
        assert manager._total_connections == 1
        assert conn in manager._connections["room1"]

        await manager.disconnect(conn)

        assert manager._total_connections == 0
        assert "room1" not in manager._connections

    async def test_connection_limit_exceeded(self, manager, mock_websocket):
        """최대 연결 수 초과"""
        manager.max_total_connections = 1
        await manager.connect("room1", "user1", mock_websocket)

        with pytest.raises(ConnectionLimitExceeded):
            await manager.connect("room2", "user2", mock_websocket)

    async def test_get_room_connections(self, manager, mock_websocket):
        """방의 연결 목록 가져오기"""
        conn1, _ = await manager.connect("room1", "user1", mock_websocket)
        conn2, _ = await manager.connect("room1", "user2", mock_websocket)

        connections = await manager.get_room_connections("room1")
        assert connections == {conn1, conn2}

    async def test_stop_closes_all_connections(self, manager):
        """stop()이 모든 연결을 종료"""
        mock_ws1 = AsyncMock()
        mock_ws1.close = AsyncMock()
        mock_ws2 = AsyncMock()
        mock_ws2.close = AsyncMock()

        conn1, _ = await manager.connect("room1", "user1", mock_ws1)
        conn2, _ = await manager.connect("room2", "user2", mock_ws2)

        await manager.stop()

        await asyncio.sleep(0.1)  # Let close tasks run
        assert conn1._is_closed is True
        assert conn2._is_closed is True
        mock_ws1.close.assert_called_once_with(
            code=WebSocketCloseCode.SERVICE_RESTART,
            reason="Server restarting",
        )
        mock_ws2.close.assert_called_once_with(
            code=WebSocketCloseCode.SERVICE_RESTART,
            reason="Server restarting",
        )
