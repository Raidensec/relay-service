class RequestError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class DeliveryFailure(Exception):
    def __init__(
        self, message: str, status_code: int | None = None, retryable: bool = True
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
