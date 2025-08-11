# IPFS Pin Manager — 设计定稿（MVP）

本文件在不修改 `design.md` 的前提下，补全其 TODO，给出最小可行实现（MVP）的技术设计：命名与状态机、模块与接口、存储与队列选型、HTTP API、TTL 配置与运行流程。

## 术语与命名
- 实体名称：`PinRecord`（每个 CID 的完整生命周期记录）。
- 状态枚举：`Status`（int 型枚举，存储高效），对外呈现为字符串，便于可观测与调试。
- 统一 REST 资源前缀：`/pins`（复数形式）。

### Status 设计（int 存储，字符串对外）
- 存储层（Pebble）与消息体使用 `int`，节省空间、序列化高效。
- API/日志/指标对外展示字符串，保证可读性。
- 通过稳定的映射保证可扩展性：新增枚举只在末尾追加，不复用旧值。

```go
// 枚举定义与映射（代码草案）
type Status int

const (
    StatusUnknown Status = iota
    StatusReceived
    StatusQueuedForPin
    StatusPinning
    StatusActive
    StatusScheduledForUnpin
    StatusUnpinning
    StatusUnpinSucceeded
    StatusDeadLetter
)

var statusToString = map[Status]string{
    StatusUnknown:           "Unknown",
    StatusReceived:          "Received",
    StatusQueuedForPin:      "QueuedForPin",
    StatusPinning:          "Pinning",
    StatusActive:           "Active",
    StatusScheduledForUnpin: "ScheduledForUnpin",
    StatusUnpinning:        "Unpinning",
    StatusUnpinSucceeded:   "UnpinSucceeded",
    StatusDeadLetter:       "DeadLetter",
}

// 对外 API 使用字符串编码（JSON），存储与 MQ 仍用 int
func (s Status) String() string { 
    if v, ok := statusToString[s]; ok { return v }
    return "Unknown" 
}
```

## 数据模型

### Protobuf 定义
```protobuf
message PinRecord {
    string cid = 1;
    int32 status = 2;  // Status 枚举
    int64 received_at = 3;
    int64 enqueued_at = 4;
    int64 pin_start_at = 5;
    int64 pin_succeeded_at = 6;
    int64 expire_at = 7;
    int64 schedule_unpin_at = 8;
    int64 unpin_start_at = 9;
    int64 unpin_succeeded_at = 10;
    int64 last_update_at = 11;
    int64 size_bytes = 12;
    int32 pin_attempt_count = 13;    // 仅记录重试次数
    int32 unpin_attempt_count = 14;  // 仅记录重试次数
}
```

### JSON 视图（API 响应）
```go
type PinRecordView struct {
    CID string `json:"cid"`
    Status string `json:"status"`  // 状态的字符串表示
    ReceivedAt time.Time `json:"received_at"`
    EnqueuedAt time.Time `json:"enqueued_at,omitempty"`
    PinStartAt time.Time `json:"pin_start_at,omitempty"`
    PinSucceededAt time.Time `json:"pin_succeeded_at,omitempty"`
    ExpireAt time.Time `json:"expire_at,omitempty"`
    ScheduleUnpinAt time.Time `json:"schedule_unpin_at,omitempty"`
    UnpinStartAt time.Time `json:"unpin_start_at,omitempty"`
    UnpinSucceededAt time.Time `json:"unpin_succeeded_at,omitempty"`
    LastUpdateAt time.Time `json:"last_update_at"`
    SizeBytes int64 `json:"size_bytes,omitempty"`
    PinAttemptCount int `json:"pin_attempt_count,omitempty"`
    UnpinAttemptCount int `json:"unpin_attempt_count,omitempty"`
}
```

### 存储索引（Pebble）
```go
// Key 前缀设计（节省空间）
const (
    KeyPrefixRecord = "p"  // pin record
    KeyPrefixStatus = "s"  // status index
    KeyPrefixExpire = "e"  // expire index
)

// 主键：p/{cid} → protobuf
// 状态索引：s/{status}/{timestamp}/{cid} → nil
// 过期索引：e/{timestamp}/{cid} → nil
```

## 状态机
- 主流程：
  - `Received` → `QueuedForPin` → `Pinning` → `Active` → `ScheduledForUnpin` → `Unpinning` → `UnpinSucceeded`
- 异常流：
  - Pin/Unpin 失败：指数退避重试；超过上限进入 `DeadLetter`
- 幂等语义：
  - `PUT /pins/{cid}` 若已为 `Active`，刷新 TTL；若在队列/处理中，不重复生产；若非 Active，按需重新入队

## 模块设计
- HTTP API 网关：提供 `PUT /pins/{cid}`、`GET /pins/{cid}`（MVP），后续可加 `DELETE /pins/{cid}`、列表查询、/metrics。
- MQ 抽象：`MessageQueue` 接口；首选实现 RabbitMQ，后续可扩展 NATS JetStream。
- 状态存储：`StateStore` 封装 Pebble，提供主键读写与状态/到期索引维护、批量写、崩溃恢复。
- IPFS 客户端：HTTP API（默认递归 pin/unpin；获取大小 `dag/stat` 或 `object/stat`）。
- TTL 策略：表驱动，进程启动时加载；`Compute(sizeBytes) → ttl`。
- Pin 工作池：并发消费 pin 队列，幂等、重试、指标、刷新 TTL。
- Unpin 工作池：定时扫描到期索引→入队→执行 unpin。
- GC 管理器：按周期触发 `repo/gc`，记录开始/结束时间、`repo/stat` 磁盘容量变化。
- 监控：Prometheus 指标导出（队列深度、成功/失败、耗时、GC 指标）。

## 接口定义
```go
type StateStore interface {
    Get(ctx context.Context, cid string) (*PinRecord, error)
    Put(ctx context.Context, rec *PinRecord) error
    Update(ctx context.Context, cid string, apply func(*PinRecord) error) error
    IndexByStatus(ctx context.Context, s Status) (Iterator[string], error)
    IndexByExpireBefore(ctx context.Context, ts int64, limit int) ([]string, error)
}

type MessageQueue interface {
    Enqueue(ctx context.Context, topic string, body []byte, opts ...EnqueueOption) error
    Dequeue(ctx context.Context, topic string, handler DeliveryHandler, opts ...ConsumeOption) error // long-running
    Stats(ctx context.Context, topic string) (QueueStats, error)
}

type IpfsClient interface {
    PinAdd(ctx context.Context, cid string) error  // 默认递归
    PinRm(ctx context.Context, cid string) error   // 默认递归
    DagStat(ctx context.Context, cid string) (DagStat, error)
    RepoGC(ctx context.Context) (GCReport, error)
    RepoStat(ctx context.Context) (RepoStat, error)
}

type TtlPolicy interface {
    Compute(sizeBytes int64) time.Duration
}
```

## 配置（YAML，启动加载）
```yaml
ipfs:
  api_addr: "http://127.0.0.1:5001"

rabbitmq:
  url: "amqp://guest:guest@127.0.0.1:5672/"
  prefetch: 32
  pin:
    exchange: "pin.exchange"
    queue: "pin.queue"
    dlx: "pin.dlx"
    retry_queue: "pin.retry.queue"
    retry_delay: "30s"   # 首次退避，可在应用层叠加指数退避
  unpin:
    exchange: "unpin.exchange"
    queue: "unpin.queue"
    dlx: "unpin.dlx"
    retry_queue: "unpin.retry.queue"
    retry_delay: "30s"

workers:
  pin_concurrency: 8
  unpin_concurrency: 4
  max_retries: 3        # 最大重试次数

gc:
  interval: "1h"        # 乐观策略，简单高效

ttl:
  table:
    - max_size: 10485760   # 0 ~ 10MB
      ttl: "2160h"        # 90d
    - max_size: 1073741824 # 10MB ~ 1GB
      ttl: "720h"         # 30d
    - max_size: 10737418240 # 1GB ~ 10GB
      ttl: "168h"         # 7d
    - max_size: -1         # > 10GB（-1 表示无上限）
      ttl: "72h"          # 3d
```

## HTTP API（MVP）
- `PUT /pins/{cid}`
  - 行为：创建或刷新 `PinRecord`；写入 Pebble；入 `pin` 队列（若未在处理中/队列中）。
  - 响应：`PinRecord` 的快照（JSON，对 `status` 使用字符串）。
- `GET /pins/{cid}`
  - 行为：查询 `PinRecord`。

示例响应：
```json
{
  "cid": "bafy...",
  "status": "Active",
  "size_bytes": 123456,
  "expire_at": "2024-12-31T23:59:59Z",
  "pin_succeeded_at": "2024-01-01T10:00:00Z"
}
```

## 运行流程（最小路径）
1) 客户端 `PUT /pins/{cid}` → 写 `PinRecord(status=Received)` → 入 `pin.queue` → 标记 `QueuedForPin`。
2) Pin Worker 消费：`Pinning` → `DagStat` 获取大小 → `TTL.Compute` → `PinAdd()` → `Active` + 维护 `expireAt` 索引。
3) 定时扫描到期索引 → 命中 `cid` 入 `unpin.queue` → 置 `ScheduledForUnpin`。
4) Unpin Worker 消费：`Unpinning` → `PinRm()` → `UnpinSucceeded`。
5) GC 管理器：周期触发 `RepoGC`，记录前后 `RepoStat`。

## 错误处理与重试
- 错误详情写入日志，不入库；记录包含请求 ID、操作类型、CID、错误信息等。
- 重试基于 `attempt_count`，超过配置的 `max_retries` 进入 `DeadLetter`。
- 日志示例：
```
level=error msg="operation failed" operation=pin cid=bafy... error="connection refused" 
request_id=req_xxx attempt=2 stage=pin_add
```

## 指标（MVP）
- pin/unpin 成功/失败计数、耗时直方图
- 队列深度（RabbitMQ stats）
- 进行中操作数
- GC 耗时、前后磁盘容量

## 选型结论
- 消息队列：首选 RabbitMQ（稳定、可视化、持久化、重试/死信完善）；后续可扩展 NATS JetStream 实现。
- 存储：Pebble（无 cgo，稳定高效），主键+二级索引方案如上。
- IPFS：HTTP API，默认递归 pin/unpin；大小通过 `dag/stat` 优先，失败退化到 `object/stat`。
- TTL：表驱动，从配置加载（不热更新）。

## 实施顺序（建议）
1) 配置、模型与 Pebble 封装（含索引）
2) RabbitMQ 生产/消费与重试骨架
3) IPFS 客户端封装
4) HTTP API（`PUT /pins/{cid}`, `GET /pins/{cid}`）
5) Pin/Unpin Worker 流程与指标
6) GC 管理器（乐观策略）