import logging

from fastapi import Depends, Path, Query, HTTPException, APIRouter
from starlette.requests import Request

from src.application.message_service import MessageService
from src.application.models import MessagesResponse

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_message_service(request: Request) -> MessageService:
    return request.app.state.message_service


@router.get("/rooms/{room_id}/messages", response_model=MessagesResponse)
async def get_messages(
    message_service: MessageService = Depends(get_message_service),
    room_id: str = Path(..., min_length=1),
    last_message_timestamp: str | None = Query(None),
    limit: int = Query(30, gt=0, le=100),
):
    try:
        return await message_service.get_room_messages_with_pagination(
            room_id=room_id,
            limit=limit,
            last_message_timestamp=last_message_timestamp,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"Failed to fetch messages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch messages")
