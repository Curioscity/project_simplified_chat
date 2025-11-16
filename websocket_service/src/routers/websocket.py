import logging

from fastapi import APIRouter, Depends, Path
from orjson import orjson
from starlette.websockets import WebSocket, WebSocketDisconnect

from src.application.chat_service import ChatService
from src.application.exceptions import ConnectionLimitExceeded, MessageHandleError
from src.common.websocket_close_code import WebSocketCloseCode

logger = logging.getLogger(__name__)

router = APIRouter()


def get_chat_service(websocket: WebSocket) -> ChatService:
    return websocket.app.state.chat_service


@router.websocket("/ws/{room_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    room_id: str = Path(..., min_length=1),
    chat_service: ChatService = Depends(get_chat_service),
):
    if websocket.app.state.is_draining:
        await websocket.close(code=WebSocketCloseCode.SERVICE_RESTART)
        return

    max_message_size = websocket.app.state.settings.MAX_MESSAGE_SIZE
    user_id = websocket.query_params.get("user_id", "anonymous")
    log_extra = {"room_id": room_id, "user_id": user_id}

    conn = None
    websocket_close_code = WebSocketCloseCode.NORMAL_CLOSURE

    try:
        try:
            conn = await chat_service.connect(room_id, user_id, websocket)
        except ConnectionLimitExceeded as e:
            await websocket.close(code=e.code, reason=e.message)
            return

        while True:
            data = await websocket.receive_bytes()

            # Rate Limit
            if conn.is_rate_limited():
                await chat_service.send_error_response(
                    conn, "rate_limited", "Too many messages", retry_after=1.0
                )
                continue

            # 메시지 크기 제한
            if len(data) > max_message_size:
                await chat_service.send_error_response(
                    conn, "message_too_large", "Message too large"
                )
                continue

            # JSON 파싱
            try:
                message_data = orjson.loads(data)
            except orjson.JSONDecodeError:
                logger.warning(f"Invalid JSON from {user_id}", extra=log_extra)
                await chat_service.send_error_response(
                    conn, "invalid_json", "Invalid JSON"
                )
                continue

            # 메시지 처리
            try:
                await chat_service.handle_message(conn, message_data)

            except MessageHandleError as e:
                await chat_service.send_error_response(
                    conn, e.error_code, e.message, retry_after=e.retry_after
                )
                if e.should_disconnect:
                    websocket_close_code = WebSocketCloseCode.SERVICE_RESTART
                    break

                continue

            except Exception as e:
                logger.error(
                    f"Message handling failed: {e}", extra=log_extra, exc_info=True
                )
                await chat_service.send_error_response(
                    conn, "internal_error", "Internal error"
                )
                websocket_close_code = WebSocketCloseCode.INTERNAL_ERROR
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {user_id}", extra=log_extra)
        websocket_close_code = WebSocketCloseCode.NORMAL_CLOSURE

    except Exception as e:
        logger.error(f"WebSocket layer error: {e}", exc_info=True)
        websocket_close_code = WebSocketCloseCode.INTERNAL_ERROR

    finally:
        if conn:
            await chat_service.disconnect(
                conn, websocket_close_code=websocket_close_code
            )
            logger.info(
                f"Connection cleaned up: {user_id}",
                extra={
                    **log_extra,
                    "close_code": websocket_close_code,
                    "close_reason": WebSocketCloseCode.get_reason(websocket_close_code),
                    "should_reconnect": WebSocketCloseCode.should_reconnect(
                        websocket_close_code
                    ),
                },
            )
