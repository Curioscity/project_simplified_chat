import asyncio
import logging
import time

from opentelemetry import propagate
from pydantic import ValidationError, BaseModel
from starlette.websockets import WebSocket

from src.application.connection_manager import Connection, ConnectionManager
from src.application.batch_publisher import BatchPublisher
from src.common.websocket_close_code import WebSocketCloseCode
from src.domain.chat_message import ChatMessage
from src.infrastructure.kafka import KafkaProducerAdapter
from src.infrastructure.otel import OTELManager
from src.infrastructure.redis import RedisAdapter
from src.application.exceptions import (
    MessageHandleError,
    ServiceUnavailableError,
    BatchPublisherFullError,
)

logger = logging.getLogger(__name__)


class ChatService:
    """채팅 서비스 레이어"""

    def __init__(
        self,
        otel_manager: OTELManager,
        conn_manager: ConnectionManager,
        batch_publisher: BatchPublisher,
        kafka_producer_adapter: KafkaProducerAdapter,
        redis_adapter: RedisAdapter,
    ):
        self.otel_manager = otel_manager
        self.conn_manager = conn_manager
        self.batch_publisher = batch_publisher
        self.kafka_producer_adapter = kafka_producer_adapter
        self.redis_adapter = redis_adapter

    async def connect(
        self, room_id: str, user_id: str, websocket: WebSocket
    ) -> Connection:
        """연결 추가"""
        conn, is_first = await self.conn_manager.connect(room_id, user_id, websocket)

        if is_first:
            try:
                await self.redis_adapter.subscribe(room_id)
                logger.info(f"Subscribed to room: {room_id}")
            except Exception as e:
                logger.error(f"Failed to subscribe to room {room_id}: {e}")
                await self.conn_manager.disconnect(
                    conn,
                    websocket_close_code=WebSocketCloseCode.INTERNAL_ERROR,
                )
                raise

        return conn

    async def disconnect(
        self, conn: Connection, websocket_close_code: int | None = None
    ):
        """연결 제거"""
        if websocket_close_code is None:
            websocket_close_code = WebSocketCloseCode.NORMAL_CLOSURE

        await self.conn_manager.disconnect(
            conn, websocket_close_code=websocket_close_code
        )

    async def handle_message(self, conn: Connection, message_data: dict):
        """
        메시지 처리

        Raises:
            ValueError: 메시지 검증 실패
            ServiceUnavailableError: 서비스 종료 중
            BatchPublisherFullError: 버퍼 가득 참
        """
        room_id = conn.room_id
        user_id = conn.user_id

        try:
            chat_message = ChatMessage(
                content=message_data["content"],
                room_id=room_id,
                user_id=user_id,
            )
        except (KeyError, ValidationError) as e:
            logger.warning(
                f"Message validation failed: {e}",
                extra={"room_id": room_id, "user_id": user_id},
            )
            raise MessageHandleError(
                message="Invalid message",
                error_code="invalid_message",
                should_disconnect=False,
            ) from e

        with self.otel_manager.tracer.start_as_current_span(
            "message.e2e",
            attributes={
                "room_id": room_id,
                "user_id": user_id,
                "message.length": len(chat_message.content),
            },
        ) as root_span:
            chat_message_dict = chat_message.model_dump()

            # 1. Batch Publisher에 추가
            with self.otel_manager.tracer.start_as_current_span(
                "message.add.batch_publisher"
            ):
                chat_message_dict["_e2e_start"] = time.monotonic()
                carrier = {}
                propagate.inject(carrier)

                try:
                    await self.batch_publisher.add(
                        chat_message_dict, trace_context=carrier
                    )

                except ServiceUnavailableError as e:
                    logger.error(
                        f"Service shutting down: {e}",
                        extra={"room_id": room_id, "user_id": user_id},
                    )
                    raise MessageHandleError(
                        message="Service unavailable",
                        error_code="service_unavailable",
                        should_disconnect=True,
                    ) from e

                except BatchPublisherFullError as e:
                    logger.warning(
                        f"Queue full: {e}",
                        extra={"room_id": room_id, "user_id": user_id},
                    )
                    raise MessageHandleError(
                        message="Server is busy",
                        error_code="server_busy",
                        should_disconnect=False,
                        retry_after=0.5,
                    ) from e

            # 2. Kafka 저장
            with self.otel_manager.tracer.start_as_current_span(
                "message.add.kafka",
            ):
                asyncio.create_task(
                    self.kafka_producer_adapter.send_chat_message(chat_message_dict)
                )

            root_span.set_attribute("message.submitted", True)

    async def send_error_response(
        self,
        conn: Connection,
        code: str,
        message: str,
        retry_after: float | None = None,
    ):
        """에러 응답"""
        try:
            error = ErrorResponse(code=code, message=message, retry_after=retry_after)
            await conn.websocket.send_json(error.model_dump())
        except Exception as e:
            logger.error(f"Failed to send error: {e}", exc_info=True)


class ErrorResponse(BaseModel):
    type: str = "error"
    code: str
    message: str
    retry_after: float | None = None
