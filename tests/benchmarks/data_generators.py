"""模拟 A 股行情数据生成器。

生成逼真的股票行情快照数据，包含：
- 股票代码 / 股票名称
- 高开低收
- 五档买价 / 五档卖价（含量）
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any


# A 股样本股票池（沪深各 10 只）
_STOCK_POOL: list[tuple[str, str, float]] = [
    # (代码, 名称, 基准价格)
    ("sh.600000", "浦发银行", 7.50),
    ("sh.600036", "招商银行", 35.20),
    ("sh.600519", "贵州茅台", 1700.00),
    ("sh.601318", "中国平安", 48.30),
    ("sh.601398", "工商银行", 5.10),
    ("sh.600276", "恒瑞医药", 42.80),
    ("sh.601012", "隆基绿能", 22.50),
    ("sh.600887", "伊利股份", 30.60),
    ("sh.601888", "中国中免", 85.40),
    ("sh.600030", "中信证券", 21.30),
    ("sz.000001", "平安银行", 11.20),
    ("sz.000333", "美的集团", 62.50),
    ("sz.000858", "五粮液", 155.00),
    ("sz.002415", "海康威视", 32.40),
    ("sz.000568", "泸州老窖", 210.00),
    ("sz.002714", "牧原股份", 42.00),
    ("sz.000725", "京东方A", 4.20),
    ("sz.002230", "科大讯飞", 55.80),
    ("sz.300750", "宁德时代", 195.00),
    ("sz.002594", "比亚迪", 260.00),
]


@dataclass
class MarketSnapshot:
    """单只股票的行情快照。"""

    code: str                     # 股票代码
    name: str                     # 股票名称
    open: float                   # 开盘价
    high: float                   # 最高价
    low: float                    # 最低价
    close: float                  # 收盘价（最新价）
    volume: int                   # 成交量
    amount: float                 # 成交额
    timestamp: float              # 时间戳
    bid_prices: list[float]       # 五档买价
    bid_volumes: list[int]        # 五档买量
    ask_prices: list[float]       # 五档卖价
    ask_volumes: list[int]        # 五档卖量

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "ts": self.timestamp,
            "bid_p": self.bid_prices,
            "bid_v": self.bid_volumes,
            "ask_p": self.ask_prices,
            "ask_v": self.ask_volumes,
        }


def generate_snapshot(
    code: str | None = None,
    base_price: float | None = None,
) -> MarketSnapshot:
    """生成单条行情快照。"""
    if code is None or base_price is None:
        code, _, base_price = random.choice(_STOCK_POOL)

    # 价格波动 ±2%
    pct = random.uniform(-0.02, 0.02)
    close = round(base_price * (1 + pct), 2)

    # 高开低保持合理关系
    spread = base_price * 0.005  # 半天振幅约 0.5%
    open_ = round(base_price + random.uniform(-spread, spread), 2)
    high = round(max(open_, close) + random.uniform(0, spread), 2)
    low = round(min(open_, close) - random.uniform(0, spread), 2)

    # 成交量/额
    volume = random.randint(100_000, 50_000_000)
    amount = round(volume * close, 2)

    # 五档盘口
    tick = 0.01 if base_price < 10 else (0.05 if base_price < 100 else 0.10)
    bid_prices = [round(close - tick * (i + 1), 2) for i in range(5)]
    ask_prices = [round(close + tick * (i + 1), 2) for i in range(5)]
    bid_volumes = [random.randint(10, 5000) for _ in range(5)]
    ask_volumes = [random.randint(10, 5000) for _ in range(5)]

    # 名称查找
    name = ""
    for c, n, _ in _STOCK_POOL:
        if c == code:
            name = n
            break

    return MarketSnapshot(
        code=code,
        name=name,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=amount,
        timestamp=time.time(),
        bid_prices=bid_prices,
        bid_volumes=bid_volumes,
        ask_prices=ask_prices,
        ask_volumes=ask_volumes,
    )


def generate_batch(
    count: int,
    codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """生成一批行情快照字典。

    Args:
        count: 生成条数
        codes: 限定股票代码列表（None 则随机选取）
    """
    if codes is not None:
        stock_map = {c: n_p for c, n_p in
                     ((c, (n, p)) for c, n, p in _STOCK_POOL)}
        result = []
        for code in (codes if codes else [s[0] for s in _STOCK_POOL]):
            if code in stock_map:
                name, base = stock_map[code]
                result.append(generate_snapshot(code, base).to_dict())
        # 如果指定 codes 但数量不够，循环补充
        while len(result) < count:
            code = random.choice(codes)
            _, base = stock_map.get(code, (None, 10.0))
            result.append(generate_snapshot(code, base).to_dict())
        return result[:count]

    return [generate_snapshot().to_dict() for _ in range(count)]


# 预生成常用数据集，避免测试中重复生成
_SINGLE_SNAPSHOT = generate_snapshot().to_dict()
_BATCH_1000 = None
_BATCH_10000 = None


def get_preset_single() -> dict[str, Any]:
    """获取预生成的单条快照。"""
    return _SINGLE_SNAPSHOT


def get_preset_batch(n: int = 1000) -> list[dict[str, Any]]:
    """获取预生成的批量快照。"""
    global _BATCH_1000, _BATCH_10000
    if n <= 1000:
        if _BATCH_1000 is None:
            _BATCH_1000 = generate_batch(1000)
        return _BATCH_1000[:n]
    else:
        if _BATCH_10000 is None:
            _BATCH_10000 = generate_batch(10000)
        return _BATCH_10000[:n]
