import asyncio
import logging
import time
from collections import deque

from starlette.websockets import WebSocket, WebSocketDisconnect

from src.application.exceptions import ConnectionLimitExceeded
from src.common.websocket_close_code import WebSocketCloseCode
from src.infrastructure.otel import OTELManager

logger = logging.getLogger(__name__)


class Connection:
    """WebSocket 연결"""

    def __init__(
        self,
        websocket: WebSocket,
        room_id: str,
        user_id: str,
        max_consecutive_failures: int = 3,
        send_timeout: float = 0.1,  # 100ms
        rate_limit_per_sec: int = 10,
    ):
        self.websocket: WebSocket = websocket
        self.room_id: str = room_id
        self.user_id: str = user_id

        # 전송 설정
        self.max_consecutive_failures = max_consecutive_failures
        self.send_timeout = send_timeout

        # 상태
        self._close_lock = asyncio.Lock()
        self.consecutive_failures: int = 0
        self._is_closed: bool = False

        # Rate limit (클라이언트가 서버로 보내는 메시지 제한)
        self.rate_limit_per_sec = rate_limit_per_sec
        self._message_sent_times: deque[float] = deque(maxlen=rate_limit_per_sec + 1)

    async def send(self, message: bytes) -> bool:
        """
        메시지를 WebSocket으로 전송
        """
        if self._is_closed:
            return False

        try:
            await asyncio.wait_for(
                self.websocket.send_bytes(message), timeout=self.send_timeout
            )

            self.consecutive_failures = 0
            return True

        except asyncio.TimeoutError:
            self.consecutive_failures += 1

            if self.consecutive_failures >= self.max_consecutive_failures:
                await self.close_websocket(
                    code=WebSocketCloseCode.TRY_AGAIN_LATER,
                    reason=f"Connection too slow ({self.consecutive_failures} timeouts)",
                )

            return False

        except WebSocketDisconnect:
            logger.info(f"Client {self.user_id} disconnected during send")
            self._is_closed = True
            return False

        except Exception as e:
            logger.warning(
                f"Send failed for {self.user_id} in {self.room_id}: {e}",
                exc_info=True,
            )
            self.consecutive_failures += 1
            return False

    async def close_websocket(
        self,
        code: int = WebSocketCloseCode.NORMAL_CLOSURE,
        reason: str | None = None,
    ):
        """연결 종료"""
        lock = self._close_lock
        async with lock:
            if self._is_closed:
                return

            reason = reason or WebSocketCloseCode.get_reason(code)
            should_reconnect = WebSocketCloseCode.should_reconnect(code)

            try:
                await self.websocket.close(code=code, reason=reason)
            except Exception as e:
                logger.debug(
                    f"WebSocket close error: {e}",
                    extra={
                        "user_id": self.user_id,
                        "room_id": self.room_id,
                        "code": code,
                        "reason": reason,
                        "should_reconnect": should_reconnect,
                    },
                )

            self._is_closed = True

    def should_close(self) -> tuple[bool, str]:
        """연결을 닫아야 하는지 판단"""
        if self._is_closed:
            return True, "already_closed"

        if self.consecutive_failures >= self.max_consecutive_failures:
            return True, "too_many_failures"

        return False, ""

    def is_rate_limited(self) -> bool:
        """
        클라이언트가 서버로 메시지를 보낼 때 rate limit 체크
        (서버 → 클라이언트 전송과는 무관)
        """
        now = time.monotonic()

        while self._message_sent_times and self._message_sent_times[0] <= now - 1.0:
            self._message_sent_times.popleft()

        if len(self._message_sent_times) >= self.rate_limit_per_sec:
            return True

        self._message_sent_times.append(now)
        return False


class ConnectionManager:
    def __init__(
        self,
        otel_manager: OTELManager,
        max_total_connections: int = 10_000,
        max_consecutive_failures: int = 3,
        send_timeout: float = 0.1,
        rate_limit_per_sec: int = 3,
        num_locks: int = 256,
    ):
        self.otel_manager = otel_manager
        self.max_total_connections = max_total_connections
        self.max_consecutive_failures = max_consecutive_failures
        self.send_timeout = send_timeout
        self.rate_limit_per_sec = rate_limit_per_sec

        # 방별 연결
        self._connections: dict[str, set[Connection]] = {}
        self._total_connections: int = 0

        # 락 풀링
        self._locks = [asyncio.Lock() for _ in range(num_locks)]

    async def start(self):
        """시작 (로그용)"""
        logger.info("ConnectionManager started")

    async def stop(self):
        """종료"""
        rooms_snapshot = list(self._connections.items())

        close_tasks = [
            conn.close_websocket(
                code=WebSocketCloseCode.SERVICE_RESTART,
                reason="Server restarting",
            )
            for _, connections in rooms_snapshot
            for conn in connections
        ]

        try:
            await asyncio.wait_for(
                asyncio.gather(*close_tasks, return_exceptions=True),
                timeout=5,
            )
        except asyncio.TimeoutError:
            logger.warning("Connection close timeout during shutdown")

        self._connections.clear()
        self._total_connections = 0
        logger.info("ConnectionManager stopped")

    def _get_room_lock(self, room_id: str) -> asyncio.Lock:
        """room_id를 해싱하여 락 반환"""
        index = hash(room_id) % len(self._locks)
        return self._locks[index]

    async def get_room_connections(self, room_id: str) -> set[Connection]:
        """방의 연결 목록 반환 (복사본)"""
        lock = self._get_room_lock(room_id)

        async with lock:
            if room_id in self._connections:
                return self._connections[room_id].copy()
            return set()

    async def connect(
        self,
        room_id: str,
        user_id: str,
        websocket: WebSocket,
    ) -> tuple[Connection, bool]:
        """새 연결 추가"""
        if self._total_connections >= self.max_total_connections:
            raise ConnectionLimitExceeded(
                code=WebSocketCloseCode.TRY_AGAIN_LATER,
                message=f"Server full ({self._total_connections}/{self.max_total_connections})",
            )

        conn = Connection(
            websocket=websocket,
            room_id=room_id,
            user_id=user_id,
            max_consecutive_failures=self.max_consecutive_failures,
            send_timeout=self.send_timeout,
            rate_limit_per_sec=self.rate_limit_per_sec,
        )

        try:
            await websocket.accept()
        except Exception as e:
            logger.error(f"WebSocket accept failed: {e}")
            await conn.close_websocket(WebSocketCloseCode.PROTOCOL_ERROR)
            raise

        # 방에 추가
        lock = self._get_room_lock(room_id)
        async with lock:
            if room_id not in self._connections:
                self._connections[room_id] = set()
                is_first = True
            else:
                is_first = False

            self._connections[room_id].add(conn)
            self._total_connections += 1
            self.otel_manager.active_connections_counter.add(1)

        logger.info(
            f"User connected: {user_id} to room {room_id}",
            extra={
                "user_id": user_id,
                "room_id": room_id,
                "total_connections": self._total_connections,
            },
        )

        return conn, is_first

    async def disconnect(
        self,
        conn: Connection,
        websocket_close_code: int = WebSocketCloseCode.NORMAL_CLOSURE,
    ) -> None:
        """연결 제거"""
        await conn.close_websocket(
            code=websocket_close_code,
            reason=WebSocketCloseCode.get_reason(websocket_close_code),
        )

        room_id = conn.room_id
        user_id = conn.user_id

        lock = self._get_room_lock(room_id)
        async with lock:
            if room_id not in self._connections:
                return

            self._connections[room_id].discard(conn)
            self._total_connections -= 1
            self.otel_manager.active_connections_counter.add(-1)

            if not self._connections[room_id]:
                del self._connections[room_id]

        logger.info(
            f"User disconnected: {user_id} from room {room_id}",
            extra={
                "user_id": user_id,
                "room_id": room_id,
                "total_connections": self._total_connections,
            },
        )
