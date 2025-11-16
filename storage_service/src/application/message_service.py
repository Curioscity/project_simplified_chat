from src.application.models import MessagesResponse, Message
from src.infrastructure.message_repository import MessageRepository


class MessageService:
    """메시지 비즈니스 로직"""

    def __init__(self, repository: MessageRepository):
        self.repository = repository

    async def get_room_messages_with_pagination(
        self,
        room_id: str,
        limit: int,
        last_message_timestamp: str | None = None,
    ) -> MessagesResponse:
        """방의 메시지를 페이지네이션하여 조회"""
        messages, is_last = await self.repository.get_room_messages(
            room_id=room_id,
            limit=limit,
            last_message_timestamp=last_message_timestamp,
        )

        message_list = [
            Message(
                id=str(message["_id"]),
                content=message["content"],
                room_id=message["room_id"],
                sender_id=message["user_id"],
                timestamp=message["timestamp"],
            )
            for message in messages
        ]

        return MessagesResponse(
            messages=message_list,
            is_last=is_last,
            last_message_timestamp=message_list[-1].timestamp if message_list else None,
        )
