import pytest
from pydantic import ValidationError

from src.domain.chat_message import ChatMessage


class TestChatMessage:
    """ChatMessage 모델 테스트"""

    def test_create_chat_message_success(self):
        """메시지 생성 성공"""
        msg = ChatMessage(
            content="  Hello  ",
            room_id="room1",
            user_id="user1",
        )
        assert msg.content == "Hello"  # strip 확인
        assert msg.room_id == "room1"
        assert msg.user_id == "user1"
        assert msg.timestamp is not None

    @pytest.mark.parametrize(
        "invalid_message",
        [
            "",  # 빈 메시지
            "   ",  # 공백만 있는 메시지
            "a" * 1001,  # 길이 초과
        ],
    )
    def test_create_chat_message_invalid_content(self, invalid_message):
        """유효하지 않은 메시지 내용"""
        with pytest.raises(ValidationError):
            ChatMessage(
                content=invalid_message,
                room_id="room1",
                user_id="user1",
            )

    def test_missing_fields(self):
        """필수 필드 누락"""
        with pytest.raises(ValidationError):
            ChatMessage(content="hello")
