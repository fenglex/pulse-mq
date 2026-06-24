"""版本号单一来源。

所有模块（publisher.__version__、admin.server.SERVER_VERSION、CLI 等）
都从这里读取，避免多处硬编码导致脱节。
"""

from __future__ import annotations

__version__ = "3.2.2"
