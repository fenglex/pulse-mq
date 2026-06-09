"""一次性下载 echarts.min.js 到项目内静态目录。

用法:
    python scripts/fetch_echarts.py

环境变量:
    ECHARTS_VERSION  覆盖默认版本（默认 5.5.0）
"""

from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

VERSION = os.environ.get("ECHARTS_VERSION", "5.5.0")
URL = f"https://cdn.jsdelivr.net/npm/echarts@{VERSION}/dist/echarts.min.js"
TARGET = Path(__file__).resolve().parent.parent / "src" / "pulsemq" / "admin" / "static" / "echarts.min.js"


def main() -> int:
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    print(f"下载 {URL} → {TARGET}", file=sys.stderr)
    try:
        with urllib.request.urlopen(URL, timeout=30) as resp:
            data = resp.read()
    except Exception as e:
        print(f"下载失败: {e}", file=sys.stderr)
        return 1
    TARGET.write_bytes(data)
    print(f"完成: {len(data):,} bytes ({len(data) / 1024:.1f} KB)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
