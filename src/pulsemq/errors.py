# src/pulsemq/errors.py
"""PulseMQ 统一异常体系 + 退出码。"""


class PulseMQError(Exception):
    exit_code: int = 1


class TransportError(PulseMQError):
    exit_code = 2


class ConnectionError(PulseMQError):  # 故意覆盖内置名；包内显式导入
    exit_code = 2


class AuthenticationError(PulseMQError):
    exit_code = 3

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


class ClientStartupError(PulseMQError):
    exit_code = 4

    def __init__(self, message: str, *, reason: str | None = None,
                 address: str | None = None, username: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.address = address
        self.username = username


class FrameError(PulseMQError):
    exit_code = 5


class SerializationError(PulseMQError):
    exit_code = 5


class ConfigurationError(PulseMQError):
    exit_code = 6


class SecurityError(PulseMQError):
    """凭据文件解析失败、哈希格式非法等安全侧错误。"""
    exit_code = 6


class ResourceExhaustedError(PulseMQError):
    exit_code = 7


def exit_code_for(exc: BaseException) -> int:
    if isinstance(exc, PulseMQError):
        return exc.exit_code
    return 1
