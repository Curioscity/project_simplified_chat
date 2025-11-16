from enum import IntEnum


class WebSocketCloseCode(IntEnum):
    """
    WebSocket Close Codes (RFC 6455 표준만 사용)

    RFC 6455: https://tools.ietf.org/html/rfc6455#section-7.4.1
    """

    # Standard codes (1000-1015) 1004는 예약됨
    NORMAL_CLOSURE = 1000  # 정상 종료
    GOING_AWAY = 1001  # 서버 종료, 브라우저 탭 닫기
    PROTOCOL_ERROR = 1002  # 프로토콜 오류
    UNSUPPORTED_DATA = 1003  # 지원하지 않는 데이터 타입
    NO_STATUS_RECEIVED = 1005  # (내부용) 상태 코드 없음
    ABNORMAL_CLOSURE = 1006  # (내부용) 비정상 종료
    INVALID_FRAME_PAYLOAD_DATA = 1007  # 잘못된 페이로드
    POLICY_VIOLATION = 1008  # 정책 위반
    MESSAGE_TOO_BIG = 1009  # 메시지 너무 큼
    MANDATORY_EXTENSION = 1010  # 필수 확장 없음
    INTERNAL_ERROR = 1011  # 내부 서버 오류
    SERVICE_RESTART = 1012  # 서버 재시작
    TRY_AGAIN_LATER = 1013  # 나중에 다시 시도
    BAD_GATEWAY = 1014  # 게이트웨이 오류
    TLS_HANDSHAKE = 1015  # (내부용) TLS 핸드셰이크 실패

    @classmethod
    def get_reason(cls, code: int) -> str:
        """Close code에 대한 설명 반환"""
        reasons = {
            cls.NORMAL_CLOSURE: "Normal closure",
            cls.GOING_AWAY: "Server is shutting down",
            cls.PROTOCOL_ERROR: "Protocol error",
            cls.UNSUPPORTED_DATA: "Unsupported data type",
            cls.INVALID_FRAME_PAYLOAD_DATA: "Invalid frame payload",
            cls.POLICY_VIOLATION: "Policy violation",
            cls.MESSAGE_TOO_BIG: "Message too big",
            cls.INTERNAL_ERROR: "Internal server error",
            cls.SERVICE_RESTART: "Service restarting",
            cls.TRY_AGAIN_LATER: "Try again later",
            cls.BAD_GATEWAY: "Bad gateway",
        }
        return reasons.get(code, f"Unknown code: {code}")

    @classmethod
    def should_reconnect(cls, code: int) -> bool:
        """클라이언트가 재연결을 시도해야 하는지 판단"""
        reconnectable_codes = {
            cls.GOING_AWAY,  # 서버 재시작 등
            cls.SERVICE_RESTART,  # 명시적 재시작
            cls.TRY_AGAIN_LATER,  # 일시적 과부하
            cls.INTERNAL_ERROR,  # 일시적 오류
            cls.ABNORMAL_CLOSURE,  # 비정상 종료
        }
        return code in reconnectable_codes
