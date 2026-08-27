# 06 路由与在线注册设计

> 源码：`src/pulsemq/routing.py`（订阅路由表）+ `src/pulsemq/control.py`（命令常量与在线注册表）

## 1. 两个纯领域模块的共同特征

- 不依赖 transport / zmq / asyncio，可单测、可被服务端与客户端同时复用（客户端 `Client` 也持有一张 `SubscriptionTable` 做本地前缀匹配）。
- 唯一写者是服务端控制面 / 客户端业务线程，读者是数据面热路径——设计均围绕「读写分离」展开。

## 2. SubscriptionTable（`routing.py`）

### 2.1 匹配语义

| pattern | 匹配的 topic |
|---------|--------------|
| `foo` | 精确 `foo` |
| `foo.*` | `foo` 及任意 `foo.<anything>`（含多级 `foo.a.b`） |

判定逻辑 `_matches`：`foo.*` 剥离 `.*` 得前缀 `foo`，匹配 `topic == "foo"` 或 `topic.startswith("foo.")`。客户端 `client._matches` 与其保持一致。

### 2.2 COW（copy-on-write）无锁读

数据结构是不可变快照 `_Index`（frozen dataclass）：

```python
_Index:
    exact:        dict[topic,   frozenset[identity]]   # 精确订阅
    wild:         dict[prefix,  frozenset[identity]]   # 前缀订阅（pattern 剥 .* 后的 prefix）
    by_identity:  dict[identity, frozenset[pattern]]   # 反查（remove / snapshot 用）
```

- **写路径**（`subscribe/unsubscribe/remove`）：持 `_write_lock`，浅拷贝三个 dict → 修改副本 → 组新 `_Index` → **原子替换 `_read_index` 引用** → `_invalidate_cache()`。写频率极低（仅订阅变更），拷贝成本可忽略。
- **读路径**（`match`）：直接读引用，无锁。GIL 保证引用赋值原子，数据面见到的快照永远自洽。

### 2.3 match 结果缓存

```python
_match_cache: {topic: (version, frozenset[identity])}
_version: int   # 写操作 +1 并清缓存
```

- 命中条件：缓存条目 version == 当前 `_version`，直接返回 frozenset——同一 topic 反复发布（典型场景）时跳过 split/join 分配与多次 dict 查找。
- 版本校验避免「写后读到陈旧结果」；version 与 `_read_index` 在写锁内一起更新，读侧最差多算一次、不会错。

`match` 的前缀展开：`foo.a.b` 依次查 `wild["foo.a"]`、`wild["foo"]`（`range(len(parts)-1, 0, -1)`，不含整串自身——整串已在 `wild[topic]` 查过一次）。

### 2.4 其他接口

- `subscribers_of(identity)`：反查某连接的订阅模式集。
- `snapshot()`：`by_identity` 的 JSON 友好视图（bytes key decode 为 str，value 排序），供 Admin `/api/v1/stats/realtime` 的 `subscriptions` 字段。
- `remove(identity)`：按 `by_identity` 反查全部 pattern 一并清除（DISCONNECT / 心跳超时路径）。

### 2.5 服务端与客户端的两种用法

| | 服务端 | 客户端 |
|---|---|---|
| identity | ROUTER bytes identity | pattern 自身（`subscribe(pattern.encode(), pattern)`，仅当索引用） |
| match 输入 | 帧头部 topic | 帧头部 topic |
| match 输出 | 待发送的 identity 集合 | 命中的 pattern id 集合 → 查 `_subscriptions` 得回调 |

## 3. control.py：命令常量 + OnlineRegistry

### 3.1 常量

- `ControlCmd`：REGISTER / HEARTBEAT / SUBSCRIBE / UNSUBSCRIBE / DISCONNECT / LATENCY_REPORT（字符串常量，作为控制帧 topic）。
- `ControlMessage(cmd, payload)`：解码后的控制消息。
- `RegisterResult`：OK / ALREADY_ONLINE / REJECTED。
- `ClientInfo`：client_id、username、endpoint、roles、topics、connected_at、last_seen。

### 3.2 OnlineRegistry：username 唯一的在线表

双索引：

```
_by_client: {client_id: ClientInfo}
_by_user:   {username: client_id}      # username → client_id，单用户单在线
```

| 方法 | 语义 |
|------|------|
| `register(info)` | username 已在 `_by_user` → `ALREADY_ONLINE`；否则双写，`last_seen` 缺省取当前时间 |
| `heartbeat(client_id)` | 刷新 `last_seen`（未知 client_id 静默忽略） |
| `get_username(client_id)` | 反查用户名（供订阅事件埋点） |
| `subscribe/unsubscribe(client_id, pattern)` | **回写 ClientInfo.topics**（set 去重 + 排序）。注册时的 topics 只是首注册快照，后续 SUBSCRIBE 必须回写，否则监控的在线快照/订阅计数与路由表脱节（源码注释明确该动机） |
| `unregister(client_id)` | 双索引删除 |
| `sweep_timeout()` | 返回并剔除 `now - last_seen > heartbeat_timeout`（默认 6s）的 ClientInfo 列表 |
| `snapshot()` | `{clients: [...]}` JSON 视图（Admin / SSE 消费） |

### 3.3 identity 与 client_id 的双键协作（服务端全景）

```
REGISTER   ──▶ registry[client_id] = ClientInfo        （字符串键）
               _ident_by_client_id[client_id] = ident   （bytes 桥接表）
               routing[topic] += ident                  （bytes 键）
HEARTBEAT  ──▶ registry.heartbeat(client_id) + credits[ident] = credit
DISCONNECT ──▶ routing.remove(ident) + 双表清理
心跳超时    ──▶ sweep 出 client_id → 反查 ident → routing.remove(ident)
```

心跳超时清理必须走反查表：registry 不知道 bytes ident，路由表不知道 client_id——`Server._ident_by_client_id` 是两者之间唯一的桥。
