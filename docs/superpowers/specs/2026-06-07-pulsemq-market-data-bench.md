# PulseMQ 真实行情数据 100k 压测设计

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用真实场景的模拟行情数据 (高开低收 + 成交量 + 成交额 + 股票代码 + 名称) 做 100k 压测, 覆盖 str (1 quote/msg) 与 DataFrame (100/1000 quotes/msg) 三种批量, 测出真实批量下的吞吐与延迟, 而非 1-row toy 数据。

**Architecture:**
- **str 路径**: 1 quote/msg, JSON 字符串 (msgspec/string 或 raw json.dumps)
- **DataFrame 路径**: msgspec.json 默认序列化, 100 或 1000 quotes/msg
- **Batcher**: 关闭 (用户指定, 测单条/单批原始场景)
- **数据生成器**: 固定 seed (random.seed(42)) 保证可复现

**Tech Stack:** Python 3.13+, msgspec, pandas, pyzmq, snappy/lz4/zstd

**Spec:** 当前文件

---

## 测试矩阵

| 维度 | 取值 |
|------|------|
| **data_types** | `str` · `df-100rows` · `df-100rows-1k` |
| **compressions** | `none` · `snappy` · `lz4` · `zstd` |
| **每组合消息数** | 100,000 |
| **总消息数** | 12 × 100,000 = **1,200,000** |
| **总 quote 数** | 100k + 10M + 100M = **~110M** |
| **Batcher** | 关闭 (batch_size=1) |
| **端口** | 18000 |
| **server** | 关闭 auth/metrics (与 100k baseline 一致) |

### 每条消息内容

| 字段 | 类型 | 范围/格式 |
|------|------|-----------|
| `code` | str | 6 位股票代码 ("600000", "000001" 等) |
| `name` | str | 股票名称 ("浦发银行", "宁德时代" 等) |
| `open` | float | 开盘价 (round 2 位小数) |
| `high` | float | 最高价 |
| `low` | float | 最低价 |
| `close` | float | 收盘价 |
| `volume` | int | 成交量 (10k-1M) |
| `turnover` | float | 成交额 (volume × avg price) |

### 股票池 (20 只, 循环使用)

```python
STOCKS = [
    ("600000", "浦发银行"), ("000001", "平安银行"), ("300750", "宁德时代"),
    ("600519", "贵州茅台"), ("000858", "五粮液"), ("601318", "中国平安"),
    ("000333", "美的集团"), ("002594", "比亚迪"), ("600276", "恒瑞医药"),
    ("000568", "泸州老窖"), ("601012", "隆基绿能"), ("002475", "立讯精密"),
    ("600030", "中信证券"), ("601888", "中国中免"), ("000063", "中兴通讯"),
    ("002714", "牧原股份"), ("600887", "伊利股份"), ("601166", "兴业银行"),
    ("000002", "万科A"), ("600585", "海螺水泥"),
]
```

### 数据生成器

```python
import random
random.seed(42)

def gen_quote(idx: int) -> dict:
    code, name = STOCKS[idx % len(STOCKS)]
    base_price = 10.0 + (idx % 1000) * 0.1
    open_ = base_price + random.uniform(-0.5, 0.5)
    close = open_ + random.uniform(-0.3, 0.3)
    high = max(open_, close) + random.uniform(0, 0.2)
    low = min(open_, close) - random.uniform(0, 0.2)
    volume = random.randint(10000, 1000000)
    turnover = volume * (high + low) / 2
    return {
        "code": code, "name": name,
        "open": round(open_, 2), "high": round(high, 2),
        "low": round(low, 2), "close": round(close, 2),
        "volume": volume, "turnover": round(turnover, 2),
    }
```

### Payload 构造

```python
def build_payload(data_type: str, idx: int):
    if data_type == "str":
        # 1 quote → JSON 字符串 (约 130 bytes)
        return json.dumps(gen_quote(idx), ensure_ascii=False)
    if data_type == "df-100rows":
        # 100 quotes → DataFrame(100, 8)
        import pandas as pd
        rows = [gen_quote(idx * 100 + j) for j in range(100)]
        return pd.DataFrame(rows)
    if data_type == "df-100rows-1k":
        # 1000 quotes → DataFrame(1000, 8)
        import pandas as pd
        rows = [gen_quote(idx * 1000 + j) for j in range(1000)]
        return pd.DataFrame(rows)
    raise ValueError(data_type)
```

### 序列化映射

| data_type | format 参数 | 走哪个 serializer | payload 大小 (估) |
|-----------|-------------|-------------------|------------------|
| `str` | None | StringSerializer (UTF-8 bytes) | ~130 bytes |
| `df-100rows` | None (默认 json) | JsonSerializer (msgspec) | ~12 KB |
| `df-100rows-1k` | None (默认 json) | JsonSerializer (msgspec) | ~120 KB |

---

## 文件结构

| 文件 | 状态 | 职责 |
|------|------|------|
| `scripts/bench_market_data.py` | 新增 | 12 组合 × 100k 压测脚本 |
| `docs/perf-market-data.md` | 新增 | 完整报告 (含 12 组合数据 + 关键洞察) |
| `docs/perf-market-data-results.md` | 新增 | 原始 12 行数据 (markdown table) |

---

## 验收清单

- [ ] 脚本可重复运行 (固定 seed, 数据完全一致)
- [ ] 12 组合全部跑通, 无 timeout
- [ ] 与之前 1-row 压测对比: df-100/df-1000 端到端吞吐与延迟变化
- [ ] 报告含:
  - 12 组合详细数据
  - 按 data_type 聚合 (3 类)
  - 按 compression 聚合 (4 类)
  - 与 1-row baseline 对比
  - 关键洞察 (真实批量下的序列化层 amortized 成本)
  - 复现命令
- [ ] commit 干净

---

## 预期收益 (相比 1-row 测试)

| 维度 | 1-row (之前) | 真实批量 (这次) |
|------|--------------|------------------|
| **df-100rows payload** | 80 B | ~12 KB |
| **df-1000rows payload** | 80 B | ~120 KB |
| **序列化层 amortized 成本** | 100% per row | 摊销到 N rows, **小 10-100x** |
| **压缩收益 (zstd 等)** | 几乎为 0 (小 payload) | **真实可见** (大 payload) |
| **sub 端 msgspec 解码** | 单次, 0.05ms | 单次, 几 ms (更大对象) |

---

## 不在范围

- 改 client API 或协议
- 改 server 实现
- 引入新依赖
