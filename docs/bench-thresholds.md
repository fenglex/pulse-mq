# PulseMQ 性能告警阈值

## 基线引用

完整基线数字见 `docs/bench-baseline.md`。以下阈值基于该基线**浮动 20%**。

## 阈值表

| 指标 | 阈值 | 检查方式 |
|------|------|----------|
| 单 pub×单 sub 吞吐 (str/none) | ≥ 基线 × 0.8 | 跑 `bench_baseline.py` |
| p99 延迟 (str/none) | ≤ 基线 × 1.2 | 同上 |
| 并发吞吐 (4 pub × 4 sub) | ≥ 基线 × 0.8 | 跑 `bench_concurrent.py` |
| 内存泄漏 (soak 1h) | 增长 < 5% | 跑 `bench_soak.py` (待加) |
| e2e 16/16 | 必须 16/16 | 跑 `test_e2e_all.py` |
| pytest | 全绿 | `uv run pytest` |

## CI 集成建议

- 每次 PR 跑 `test_e2e_all.py` + `pytest`（耗时约 1 分钟）
- 每日定时跑 `bench_baseline.py`（耗时约 5 分钟），不通过则报警
- 每周跑 `bench_soak.py` 1h 抽检

## 调整阈值

阈值用倍数表达，便于跨硬件。基线重测时同步更新 `docs/bench-baseline.md`，
阈值保持"基线 × N"形式即可。
