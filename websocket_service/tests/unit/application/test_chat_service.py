import pytest
from unittest.mock import AsyncMock

import pytest_asyncio

from src.application.chat_service import ChatService
from src.application.exceptions import MessageHandleError


@pytest_asyncio.fixture(autouse=True)
def mock_conn_manager():
    mock = AsyncMock()
    mock.connect = AsyncMock(return_value=(AsyncMock(), True))
    mock.disconnect = AsyncMock()
    return mock


@pytest_asyncio.fixture(autouse=True)
def mock_batch_publisher():
    return AsyncMock()


@pytest_asyncio.fixture(autouse=True)
def mock_kafka_producer():
    return AsyncMock()


@pytest_asyncio.fixture(autouse=True)
def mock_redis_adapter():
    return AsyncMock()


@pytest_asyncio.fixture(autouse=True)
def chat_service(
    mock_otel_manager,
    mock_conn_manager,
    mock_batch_publisher,
    mock_kafka_producer,
    mock_redis_adapter,
):
    """ChatService 인스턴스"""
    return ChatService(
        otel_manager=mock_otel_manager,
        conn_manager=mock_conn_manager,
        batch_publisher=mock_batch_publisher,
        kafka_producer_adapter=mock_kafka_producer,
        redis_adapter=mock_redis_adapter,
    )


@pytest.mark.asyncio
class TestChatService:
    """ChatService 테스트"""

    async def test_connect_success(
        self, chat_service, mock_conn_manager, mock_redis_adapter
    ):
        """연결 성공"""
        result_conn = await chat_service.connect("room1", "user1", AsyncMock())

        assert result_conn == mock_conn_manager.connect.return_value[0]
        mock_conn_manager.connect.assert_called_once()
        mock_redis_adapter.subscribe.assert_called_once_with("room1")

    async def test_connect_redis_subscribe_fails(
        self, chat_service, mock_conn_manager, mock_redis_adapter
    ):
        """Redis 구독 실패 시 연결 롤백"""
        mock_redis_adapter.subscribe.side_effect = Exception("Redis down")
        conn = mock_conn_manager.connect.return_value[0]

        with pytest.raises(Exception, match="Redis down"):
            await chat_service.connect("room1", "user1", AsyncMock())

        mock_conn_manager.disconnect.assert_called_once_with(
            conn, websocket_close_code=1011
        )

    async def test_disconnect(self, chat_service, mock_conn_manager):
        """연결 종료"""
        conn = AsyncMock()
        await chat_service.disconnect(conn)
        mock_conn_manager.disconnect.assert_called_once_with(
            conn, websocket_close_code=1000
        )

    async def test_handle_message_success(
        self, chat_service, mock_batch_publisher, mock_kafka_producer
    ):
        """메시지 처리 성공"""
        conn = AsyncMock()
        conn.room_id = "room1"
        conn.user_id = "user1"
        message_data = {"content": "hello"}

        await chat_service.handle_message(conn, message_data)

        mock_batch_publisher.add.assert_called_once()
        mock_kafka_producer.send_chat_message.assert_called_once()

    @pytest.mark.parametrize(
        "invalid_data",
        [
            {"msg": "hello"},  # 'message' 키 없음
            {"message": "   "},  # 내용이 공백
        ],
    )
    async def test_handle_invalid_message(self, chat_service, invalid_data):
        """잘못된 형식의 메시지 처리"""
        conn = AsyncMock()
        conn.room_id = "room1"
        conn.user_id = "user1"

        with pytest.raises(MessageHandleError):
            await chat_service.handle_message(conn, invalid_data)

    @pytest.mark.asyncio
    async def test_send_error_response(self, chat_service):
        """에러 메시지 전송"""
        conn = AsyncMock()
        conn.websocket.send_json = AsyncMock(return_value=True)

        await chat_service.send_error_response(conn, "error code", "error content")

        conn.websocket.send_json.assert_called_once()
