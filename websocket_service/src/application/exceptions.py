class ConnectionLimitExceeded(Exception):
    """커넥션 리미트가 넘었을 때 발생하는 에러"""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class BatchPublisherError(Exception):
    """BatchPublisher 관련 기본 에러"""

    pass


class ServiceUnavailableError(BatchPublisherError):
    """서비스가 종료 중이거나 사용 불가능할 때"""

    def __init__(self, message: str = "서비스가 일시적으로 사용 불가능합니다"):
        self.message = message
        super().__init__(self.message)


class BatchPublisherFullError(BatchPublisherError):
    """메시지 버퍼가 가득 찼을 때"""

    def __init__(self, message: str = "메시지 버퍼가 가득 찼습니다"):
        self.message = message
        super().__init__(self.message)


class MessageHandleError(Exception):
    """메시지 처리 중 발생하는 에러"""

    def __init__(
        self,
        message: str,
        error_code: str,
        should_disconnect: bool = False,
        retry_after: float | None = None,
    ):
        self.message = message
        self.error_code = error_code
        self.should_disconnect = should_disconnect
        self.retry_after = retry_after
        super().__init__(message)
