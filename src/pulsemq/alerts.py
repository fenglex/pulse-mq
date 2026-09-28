"""告警规则引擎（9.2.8）：阈值规则 → webhook POST / 日志事件。

设计原则：规则固定、阈值可配（ServerConfig.alert_*，环境变量可覆盖）；
无 webhook 配置时降级为 log_event WARNING（运维可在日志/事件流看到）。
每规则独立冷却时间，避免风暴；check() 由服务端心跳级任务每秒调用。

输入快照（Server._alert_snapshot 组装）：
    drops_last_min_total  上一分钟丢弃总量（DropStats）
    gaps_total            缺口累计总量（GapStats）
    buffer_max_age_s      最深订阅者缓冲的最老帧年龄（BufferManager）
    hb_kicks              心跳踢线累计（HeartbeatMonitor）
    starved_per_s         信用窗口拦截速率（DataPlaneStats）
    loop_max_ms           数据面循环单轮最大耗时（DataPlaneStats）
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request

from pulsemq.logging_setup import log_event, logger

_WEBHOOK_TIMEOUT_S = 3.0


class AlertManager:
    """固定规则集的阈值告警。"""

    def __init__(self, *, webhook: str = "", cooldown_s: float = 60.0,
                 drop_per_min: int = 1000, gap_per_min: int = 1000,
                 buffer_age_s: float = 5.0, starved_per_s: float = 100.0,
                 loop_stall_ms: float = 1000.0,
                 heartbeat_kick: bool = True) -> None:
        self.webhook = webhook
        self.cooldown_s = cooldown_s
        self.thresholds = {
            "drop_per_min": drop_per_min,
            "gap_per_min": gap_per_min,
            "buffer_age_s": buffer_age_s,
            "starved_per_s": starved_per_s,
            "loop_stall_ms": loop_stall_ms,
        }
        self.heartbeat_kick_enabled = heartbeat_kick
        self._last_fire: dict[str, float] = {}
        # 缺口增量基线（gap 规则用窗口增量，不是累计值）
        self._gaps_base: float | None = None
        self._gaps_base_ts: float | None = None

    async def check(self, snap: dict) -> list[dict]:
        """评估规则，返回本次触发的告警列表（冷却中的不重发）。"""
        fired: list[dict] = []
        now = time.monotonic()

        # 丢弃突增（上一分钟总量 ≥ 阈值；阈值 ≤0 = 规则关闭）
        dlm = snap.get("drops_last_min_total", 0)
        if self.thresholds["drop_per_min"] > 0 and dlm >= self.thresholds["drop_per_min"]:
            fired.append({"rule": "drops_burst",
                          "detail": f"近 1 分钟丢弃 {dlm} 帧",
                          "value": dlm})

        # 缺口突增：按窗口增量折算每分钟速率（阈值 ≤0 = 规则关闭）
        gt = snap.get("gaps_total", 0)
        if self._gaps_base_ts is not None and gt >= self._gaps_base:
            elapsed = max(1e-9, now - self._gaps_base_ts)
            per_min = (gt - self._gaps_base) / elapsed * 60.0
            if (self.thresholds["gap_per_min"] > 0
                    and per_min >= self.thresholds["gap_per_min"]):
                fired.append({"rule": "gap_burst",
                              "detail": f"缺口增速 {per_min:.0f} 帧/分钟",
                              "value": round(per_min, 1)})
        self._gaps_base = gt
        self._gaps_base_ts = now

        # 缓冲积压变老（最老帧年龄 ≥ 阈值秒；阈值 ≤0 = 规则关闭）
        age = snap.get("buffer_max_age_s", 0.0)
        if self.thresholds["buffer_age_s"] > 0 and age >= self.thresholds["buffer_age_s"]:
            fired.append({"rule": "buffer_backlog",
                          "detail": f"订阅者缓冲最老积压 {age:.1f}s",
                          "value": round(age, 2)})

        # 信用窗口持续拦截（消费端被流控掐住的强度；阈值 ≤0 = 规则关闭）
        sps = snap.get("starved_per_s", 0.0)
        if (self.thresholds["starved_per_s"] > 0
                and sps >= self.thresholds["starved_per_s"]):
            fired.append({"rule": "credit_starved",
                          "detail": f"信用窗口拦截 {sps:.0f} 次/秒",
                          "value": round(sps, 1)})

        # 数据面循环停顿（单轮 poll+drain 耗时异常；阈值 ≤0 = 规则关闭）
        lmm = snap.get("loop_max_ms", 0.0)
        if (self.thresholds["loop_stall_ms"] > 0
                and lmm >= self.thresholds["loop_stall_ms"]):
            fired.append({"rule": "dataplane_stall",
                          "detail": f"数据面单轮循环 {lmm:.0f}ms（疑似停顿）",
                          "value": round(lmm, 1)})

        # 心跳踢线（每次踢线都值得知道）
        if self.heartbeat_kick_enabled and snap.get("hb_kick_delta", 0) > 0:
            fired.append({"rule": "client_kicked",
                          "detail": f"心跳超时踢线 {snap['hb_kick_delta']} 个客户端",
                          "value": snap["hb_kick_delta"]})

        out: list[dict] = []
        for a in fired:
            last = self._last_fire.get(a["rule"], 0.0)
            if now - last < self.cooldown_s:
                continue
            self._last_fire[a["rule"]] = now
            a["ts"] = time.time()
            a["level"] = "WARNING"
            out.append(a)
            log_event("WARNING", "ALERT", rule=a["rule"], detail=a["detail"])
            if self.webhook:
                await self._post_webhook(a)
        return out

    async def _post_webhook(self, alert: dict) -> None:
        """POST JSON 到 webhook（线程池执行，3s 超时；失败仅 debug 日志）。"""
        try:
            req = urllib.request.Request(
                self.webhook,
                data=json.dumps(alert, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=_WEBHOOK_TIMEOUT_S),
            )
        except Exception:
            logger.debug("告警 webhook 发送失败", exc_info=True)
