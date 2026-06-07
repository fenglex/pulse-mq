# ZMQ IO 线程化设计 — 消除 asyncio + zmq.asyncio 每消息 await 切换

**日期**: 2026-06-07
**状态**: approved (per user: 自己执行)
**作者**: brainstorming session

## 背景与动机

当前 PulseMQ 主路径使用 `zmq.asyncio.Socket`,在 asyncio 事件循环中 `await zmq.recv()` / `await zmq.send()`。实测数据(Windows + SelectorEventLoop):

| 指标 | 当前 | 说明 |
|------|------|------|
| 单消息 in-flight 时间 | 24.3 μs | 6 个 await 切换点 |
| E2E 吞吐 (1 pub + 1 sub) | 32k msg/s | send 165k msg/s, 但广播路径瓶颈 |
| Server recv+dispatch 等待 | ~6μs/msg | engine 主循环 1 个 await |
| Server broadcast 等待 | ~6μs/msg | broadcast_loop 1 个 await |

每消息有 2-3 个 zmq.asyncio 的 await,每次 `await zmq.asyncio.Socket.send_multipart` 走 `_add_send_event` 走事件循环 + I/O thread 回调,**这是当前架构的硬瓶颈**。

## 目标

把 ZMQ 收发 IO 从 asyncio 主循环移到独立 Python 线程,asyncio 主循环只做 decode + dispatch + 入 broadcast queue。预期:

| 指标 | 当前 | 目标 | 提升 |
|------|------|------|------|
| E2E 吞吐 | 32k msg/s | 80-150k msg/s | **3-5x** |
| in-flight 时间 | 24μs | 6-10μs | **3-4x** |
| 客户端 send | 不变 | 不变 | 0 |

## 架构

```
┌────────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│ ZMQ Recv Thread    │         │ Engine Async     │         │ ZMQ Broadcast    │
│ (Python threading) │         │ Main Loop        │         │ Thread           │
│                    │         │                  │         │                  │
│ zmq.Socket(ROUTER) │─frame─>│ asyncio.to_thread │─decode─>│ (Python thread)  │
│ recv_multipart()   │         │   (recv_q.get)   │ dispatch│ broadcast_q.get  │
│                    │         │                  │ record  │   xpub.send      │
│ + send_queue 处理 │         │ push broadcast ───┼────────>│                  │
│   error/auth/sync  │         │   frames         │         │ + 周期性 poll    │
└────────────────────┘         └──────────────────┘         │   XPUB subscribe  │
                                                              │   ack            │
                                                              └──────────────────┘
```

**关键边界**:
- **IO Thread** 只做 zmq 操作,没有业务逻辑
- **Main Async Loop** 只做 decode + dispatch + 业务 metrics + 入 broadcast queue
- **IO Thread ↔ Main Loop** 通过 `queue.Queue` (thread-safe) 通信
- **Main Loop 调 zmq 收/发** → 全部走 `asyncio.to_thread(queue.get/put)`

## 组件

### 1. `ZmqRecvThread` (Python threading.Thread)
- 持有 `zmq.Socket(ROUTER)` (sync API)
- 启动流程: `start()` 在调用线程做 `bind()` + `setsockopt()`,然后启动 thread
- 主循环 (`run`):
  ```
  while not stop_event.is_set():
      try:
          frames = self._socket.recv_multipart()  # 阻塞, 释放 GIL
      except zmq.Again:  # EAGAIN from poll timeout
          continue
      except zmq.ZMQError:
          log + break
      self._recv_queue.put(frames)  # 阻塞, 释放 GIL
  ```
- 同时处理 outbound send_queue (AUTH 响应, ERROR 帧等 server.py 主动 send):
  ```
  if not send_queue.empty() and socket.poll(SNDHWM check):
      identity, frames = send_queue.get_nowait()
      socket.send_multipart([identity, b""] + frames)
  ```
  用 `zmq.Poller` 同时监听 POLLIN/POLLOUT。

### 2. `ZmqBroadcastThread` (Python threading.Thread)
- 持有 `zmq.Socket(XPUB)` (sync API)
- 主循环:
  ```
  while not stop_event.is_set():
      try:
          frames = self._broadcast_queue.get(timeout=0.1)  # 阻塞 + 超时
      except Empty:  # timeout
          # 周期性 poll XPUB 的 SUBSCRIBE 确认
          if socket.poll(0):
              socket.recv_multipart()
          continue
      self._socket.send_multipart(frames)
  ```

### 3. `ZmqTransport` 改造
保持外部 API 不变,Engine 调用方无需感知:

```python
class ZmqTransport:
    def __init__(self, config):
        self._config = config
        self._ctx: zmq.Context | None = None
        self._router: zmq.Socket | None = None  # sync
        self._xpub: zmq.Socket | None = None    # sync
        # thread-safe queues
        self._recv_queue: queue.Queue[list[bytes] | None] = queue.Queue()
        self._broadcast_queue: queue.Queue[list[bytes] | None] = queue.Queue()
        self._send_queue: queue.Queue[tuple[bytes, list[bytes]]] = queue.Queue()
        # threads
        self._recv_thread: ZmqRecvThread | None = None
        self._broadcast_thread: ZmqBroadcastThread | None = None
        self._stop_event = threading.Event()
    
    async def start(self):
        self._ctx = zmq.Context()
        self._router = self._ctx.socket(zmq.ROUTER)
        # setsockopt... bind...
        self._xpub = self._ctx.socket(zmq.XPUB)
        # setsockopt... bind...
        self._stop_event.clear()
        self._recv_queue = queue.Queue()
        self._broadcast_queue = queue.Queue()
        self._send_queue = queue.Queue()
        self._recv_thread = ZmqRecvThread(self._router, self._recv_queue, self._send_queue, self._stop_event)
        self._recv_thread.start()
        self._broadcast_thread = ZmqBroadcastThread(self._xpub, self._broadcast_queue, self._stop_event)
        self._broadcast_thread.start()
    
    async def stop(self, linger_ms=2000):
        # 1. 放哨兵, 线程会自己退出
        self._stop_event.set()
        self._recv_queue.put(None)
        self._broadcast_queue.put(None)
        # 2. 等线程结束
        if self._recv_thread:
            self._recv_thread.join(timeout=2.0)
        if self._broadcast_thread:
            self._broadcast_thread.join(timeout=2.0)
        # 3. 关 socket
        if self._router:
            self._router.close(linger=linger_ms)
            self._router = None
        if self._xpub:
            self._xpub.close(linger=linger_ms)
            self._xpub = None
        if self._ctx:
            self._ctx.term()
            self._ctx = None
    
    async def recv(self) -> list[bytes]:
        # 阻塞, 但在 to_thread 里, 不阻塞 event loop
        return await asyncio.to_thread(self._recv_queue.get)
    
    async def broadcast(self, frames: list[bytes]):
        # queue.Queue.put 是 thread-safe, 直接调即可
        self._broadcast_queue.put(frames)
    
    async def send(self, identity: bytes, frames: list[bytes]):
        self._send_queue.put((identity, frames))
```

### 4. `Engine` 调整
- 删 `Engine._broadcast_loop` (XPUB 发送在 BroadcastThread)
- 主循环 recv 改为 `await transport.recv()`(已通过 to_thread 包装,无 zmq await)
- dispatch 逻辑不变
- metrics flusher 不变

```python
async def run(self):
    # ...
    while self._running:
        # 1. 背压检查 (不变)
        # 2. 双缓冲消费 (不变)
        # 3. recv (原 await transport.recv() 不变, 内部已 thread 化)
        frames = await self._transport.recv()
        # 4. dispatch (不变)
        await self._dispatch_one(frames)
```

## 文件变更

| 文件 | 变更 |
|------|------|
| `src/pulsemq/transport/zmq_transport.py` | 大改: 引入 ZmqRecvThread / ZmqBroadcastThread, sync zmq.Socket |
| `src/pulsemq/engine/engine.py` | 删 `_broadcast_loop` |
| `tests/integration/test_engine_transport.py` | 验证 transport 行为不变 |
| `tests/unit/test_engine.py` | 验证 engine 行为不变 |

## 保留的兼容性

- `ZmqTransport` 的所有 `async` 方法签名保持不变
- `_router` / `_xpub` 属性保留(server.py 仍可访问)
- `setsockopt` 在 `start()` 之前/期间调用,server.py 的 `ROUTER_NOTIFY` 仍可工作
- `monitor_socket` 在 server.py 单独使用 `zmq.asyncio.Socket`,**保持异步**(不与新 IO thread 冲突)

## 风险

1. **GIL**: 多线程 + GIL,但 zmq 释放 GIL,Python 业务代码短暂持 GIL,不是瓶颈
2. **queue.Queue 锁**: 高 QPS 下 queue.Queue 的 put/get 有锁竞争,但远比 zmq.asyncio await 便宜
3. **asyncio.to_thread 调度**: 每次 to_thread 创建新线程上下文,有少量开销,但在主循环中能摊销
4. **启动顺序**: 必须在 `bind()` 之后才能 `start()` thread,否则 thread 收不到连接

## 性能预期

| 路径 | 当前 (asyncio) | 改造后 (thread) | 提升 |
|------|---------------|-----------------|------|
| Server recv (await) | 6μs | ~2μs (to_thread) | 3x |
| Server broadcast (await) | 6μs | ~1μs (queue put) | 6x |
| 总 in-flight | 24μs | 8-12μs | 2-3x |
| E2E 吞吐 (1+1) | 32k msg/s | **80-150k msg/s** | 3-5x |

Linux + uvloop + epoll 进一步加成 → 150-300k msg/s。

## 验收

- 全量测试 522+ passed, 0 failed
- E2E 16/16 passed
- 1w E2E 吞吐 ≥ 80k msg/s (vs 当前 32k)
- 不影响 fast path 正确性, metrics 累积仍生效

## 不在范围

- Cython 扩展(留给下一轮)
- Linux uvloop 调优(本次仍测 Windows)
- 客户端 API 变更(`publish_many()` 是单独的设计)
- 协议层(完全不动)
