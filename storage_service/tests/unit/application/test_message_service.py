import pytest
from unittest.mock import AsyncMock
from src.application.message_service import MessageService
from src.application.models import MessagesResponse
from src.infrastructure.message_repository import MessageRepository


@pytest.fixture
def mock_message_repository():
    """Fixture that provides a mock MessageRepository."""
    return AsyncMock(spec=MessageRepository)


@pytest.fixture
def message_service(mock_message_repository):
    """Fixture that provides a MessageService instance with a mocked repository."""
    return MessageService(repository=mock_message_repository)


@pytest.mark.asyncio
async def test_returns_empty_response_when_no_messages(
    message_service, mock_message_repository
):
    """빈 방에 대한 메시지 조회 시 빈 응답 반환"""
    room_id = "room123"
    mock_message_repository.get_room_messages.return_value = ([], True)

    response = await message_service.get_room_messages_with_pagination(
        room_id=room_id, limit=10
    )

    assert isinstance(response, MessagesResponse)
    assert response.messages == []
    assert response.is_last is True
    assert response.last_message_timestamp is None
    mock_message_repository.get_room_messages.assert_called_once_with(
        room_id=room_id, limit=10, last_message_timestamp=None
    )


@pytest.mark.asyncio
async def test_returns_paginated_messages_with_metadata(
    message_service, mock_message_repository
):
    """메시지 목록과 페이지네이션 메타데이터 반환"""
    room_id = "room123"
    mock_messages = [
        {
            "_id": "msg1",
            "room_id": room_id,
            "content": "Hello",
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:01Z",
        },
        {
            "_id": "msg2",
            "room_id": room_id,
            "content": "Hi",
            "user_id": "user2",
            "timestamp": "2023-01-01T10:00:02Z",
        },
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, False)

    response = await message_service.get_room_messages_with_pagination(
        room_id=room_id, limit=2, last_message_timestamp="2023-01-01T10:00:00Z"
    )

    assert len(response.messages) == 2
    assert response.is_last is False
    assert response.last_message_timestamp == "2023-01-01T10:00:02Z"

    # 메시지 내용 검증 - 개별 필드 확인
    assert response.messages[0].id == "msg1"
    assert response.messages[0].content == "Hello"
    assert response.messages[0].sender_id == "user1"
    assert response.messages[1].id == "msg2"
    assert response.messages[1].content == "Hi"


@pytest.mark.asyncio
async def test_marks_last_page_when_no_more_messages(
    message_service, mock_message_repository
):
    """마지막 페이지에서 is_last=True 반환"""
    room_id = "room456"
    mock_messages = [
        {
            "_id": "msg3",
            "room_id": room_id,
            "content": "Bye",
            "user_id": "user3",
            "timestamp": "2023-01-01T11:00:01Z",
        }
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, True)

    response = await message_service.get_room_messages_with_pagination(
        room_id=room_id, limit=1, last_message_timestamp="2023-01-01T11:00:00Z"
    )

    assert response.is_last is True
    assert len(response.messages) == 1
    assert response.last_message_timestamp == "2023-01-01T11:00:01Z"


@pytest.mark.asyncio
async def test_handles_single_message(message_service, mock_message_repository):
    """단일 메시지 처리"""
    mock_messages = [
        {
            "_id": "msg1",
            "room_id": "room1",
            "content": "Solo",
            "user_id": "user1",
            "timestamp": "2023-01-01T12:00:00Z",
        }
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, True)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=10
    )

    assert len(response.messages) == 1
    assert response.last_message_timestamp == "2023-01-01T12:00:00Z"


@pytest.mark.asyncio
async def test_preserves_message_order(message_service, mock_message_repository):
    """메시지 순서 유지 확인"""
    mock_messages = [
        {
            "_id": f"msg{i}",
            "room_id": "room1",
            "content": f"Message {i}",
            "user_id": "user1",
            "timestamp": f"2023-01-01T10:00:0{i}Z",
        }
        for i in range(5)
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, False)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=5
    )

    # 순서가 유지되는지 확인
    for i, msg in enumerate(response.messages):
        assert msg.id == f"msg{i}"
        assert msg.content == f"Message {i}"


@pytest.mark.asyncio
async def test_converts_objectid_to_string(message_service, mock_message_repository):
    """ObjectId를 문자열로 변환 확인"""
    from bson import ObjectId

    object_id = ObjectId()
    mock_messages = [
        {
            "_id": object_id,
            "room_id": "room1",
            "content": "Test",
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:00Z",
        }
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, True)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=10
    )

    assert response.messages[0].id == str(object_id)
    assert isinstance(response.messages[0].id, str)


@pytest.mark.asyncio
async def test_raises_error_when_repository_fails(
    message_service, mock_message_repository
):
    """Repository 오류 시 예외 전파"""
    mock_message_repository.get_room_messages.side_effect = Exception(
        "Database connection failed"
    )

    with pytest.raises(Exception, match="Database connection failed"):
        await message_service.get_room_messages_with_pagination(
            room_id="room1", limit=10
        )


@pytest.mark.asyncio
async def test_handles_missing_fields_gracefully(
    message_service, mock_message_repository
):
    """필수 필드 누락 시 KeyError 발생 (현재 구현 기준)"""
    mock_messages = [
        {
            "_id": "msg1",
            "room_id": "room1",
            # "content" 필드 누락
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:00Z",
        }
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, True)

    with pytest.raises(KeyError):
        await message_service.get_room_messages_with_pagination(
            room_id="room1", limit=10
        )


@pytest.mark.asyncio
async def test_handles_large_batch_size(message_service, mock_message_repository):
    """대량 메시지 처리"""
    mock_messages = [
        {
            "_id": f"msg{i}",
            "room_id": "room1",
            "content": f"Message {i}",
            "user_id": "user1",
            "timestamp": f"2023-01-01T10:00:{i:02d}Z",
        }
        for i in range(1000)
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, False)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=1000
    )

    assert len(response.messages) == 1000
    assert response.last_message_timestamp == "2023-01-01T10:00:999Z"


@pytest.mark.asyncio
async def test_handles_zero_limit(message_service, mock_message_repository):
    """limit=0 처리 (Repository 레벨에서 처리되어야 함)"""
    mock_message_repository.get_room_messages.return_value = ([], True)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=0
    )

    assert response.messages == []
    mock_message_repository.get_room_messages.assert_called_once_with(
        room_id="room1", limit=0, last_message_timestamp=None
    )


@pytest.mark.asyncio
async def test_pagination_flow_multiple_pages(message_service, mock_message_repository):
    """여러 페이지에 걸친 페이지네이션 플로우"""
    # 첫 번째 페이지
    first_page = [
        {
            "_id": "msg1",
            "room_id": "room1",
            "content": "First",
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:01Z",
        },
        {
            "_id": "msg2",
            "room_id": "room1",
            "content": "Second",
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:02Z",
        },
    ]

    # 두 번째 페이지
    second_page = [
        {
            "_id": "msg3",
            "room_id": "room1",
            "content": "Third",
            "user_id": "user1",
            "timestamp": "2023-01-01T10:00:03Z",
        }
    ]

    # 첫 번째 호출
    mock_message_repository.get_room_messages.return_value = (first_page, False)
    response1 = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=2
    )

    assert len(response1.messages) == 2
    assert response1.is_last is False
    last_ts = response1.last_message_timestamp

    # 두 번째 호출 (last_message_timestamp 사용)
    mock_message_repository.get_room_messages.return_value = (second_page, True)
    response2 = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=2, last_message_timestamp=last_ts
    )

    assert len(response2.messages) == 1
    assert response2.is_last is True

    # 두 번째 호출에서 올바른 timestamp를 전달했는지 확인
    assert (
        mock_message_repository.get_room_messages.call_args_list[1][1][
            "last_message_timestamp"
        ]
        == "2023-01-01T10:00:02Z"
    )


@pytest.mark.parametrize(
    "limit,expected_count",
    [
        (1, 1),
        (5, 5),
        (10, 10),
        (100, 100),
    ],
)
@pytest.mark.asyncio
async def test_respects_limit_parameter(
    limit, expected_count, message_service, mock_message_repository
):
    """다양한 limit 값 테스트"""
    mock_messages = [
        {
            "_id": f"msg{i}",
            "room_id": "room1",
            "content": f"Message {i}",
            "user_id": "user1",
            "timestamp": f"2023-01-01T10:00:{i:02d}Z",
        }
        for i in range(expected_count)
    ]
    mock_message_repository.get_room_messages.return_value = (mock_messages, True)

    response = await message_service.get_room_messages_with_pagination(
        room_id="room1", limit=limit
    )

    assert len(response.messages) == expected_count
    mock_message_repository.get_room_messages.assert_called_once()
