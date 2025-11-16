from pydantic import BaseModel, Field, ConfigDict


class Message(BaseModel):
    id: str = Field(..., alias="_id")
    room_id: str
    content: str
    sender_id: str  # API 응답용: user_id에 매칭
    timestamp: str

    model_config = ConfigDict(
        populate_by_name=True,
    )


class MessagesResponse(BaseModel):
    messages: list[Message]
    is_last: bool
    last_message_timestamp: str | None = None
