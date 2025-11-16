from datetime import datetime, UTC

from pydantic import BaseModel, Field, field_validator


class ChatMessage(BaseModel):
    """채팅 메시지 모델"""

    content: str = Field(..., min_length=1, max_length=1000)
    room_id: str
    user_id: str
    timestamp: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())

    @field_validator("content")
    @classmethod
    def validate_content(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Message cannot be empty")
        return v.strip()
