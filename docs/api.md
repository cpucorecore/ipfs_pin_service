IPFS Pin Service API 文档

- 基础地址（默认）: `http://localhost:8081`
- 认证: 无
- 数据格式: `application/json`

资源模型: PinRecordView（响应体）
- `cid` string: 内容 CID
- `status` string: 记录状态（`Active`/`Pinning`/`PinSucceeded`/`PinFailed`/`ScheduledForUnpin`/`Unpinning`/`UnpinSucceeded`/`DeadLetter`/`Unknown`）
- `received_at` string: 接收时间
- `enqueued_at` string (optional): 入队时间
- `pin_start_at` string (optional): 开始 Pin 的时间
- `pin_succeeded_at` string (optional): Pin 完成时间
- `expire_at` string (optional): 过期时间
- `schedule_unpin_at` string (optional): 计划 Unpin 时间
- `unpin_start_at` string (optional): 开始 Unpin 的时间
- `unpin_succeeded_at` string (optional): Unpin 完成时间
- `last_update_at` string: 最后更新时间
- `size_bytes` int64: 文件大小（字节）
- `size_human` string: 文件大小（人类可读）
- `pin_attempt_count` int32: Pin 尝试次数
- `unpin_attempt_count` int32: Unpin 尝试次数
- `ttl` string (optional): 剩余 TTL（仅在未过期时）
- `age` string: 随状态显示时长（Pinning: 从 `pin_start_at`；Unpinning: 从 `unpin_start_at`；Active/PinSucceeded: 从 `pin_succeeded_at`；其他状态为空）

时间格式默认为 ISO8601。`/pins/{cid}` 的 GET 接口可通过 `time_format` 参数选择 `iso`/`unix`/`human`。

PUT /pins/{cid}
请求服务进行 Pin 操作（若已存在记录则触发刷新/重试流程）。

- 路径参数
  - `cid` string: 有效的 IPFS CID
- 查询参数
  - `size` int64 (可选): 文件大小（字节数，若已知）

- 成功响应
  - 状态码: `202 Accepted`
  - Body: `PinRecordView`

- 失败响应
  - `400 Bad Request`: CID 无效或 `size` 参数非法
  - `500 Internal Server Error`: 内部错误（存储/入队失败等）

- 示例
```bash
curl -X PUT "http://localhost:8081/pins/bafy...cid..." \
  -H 'Content-Type: application/json' \
  --silent | jq
```
携带已知大小：
```bash
curl -X PUT "http://localhost:8081/pins/bafy...cid...?size=1048576" --silent | jq
```

GET /pins/{cid}
查询 CID 当前 Pin 状态与元数据。

- 路径参数
  - `cid` string: 有效的 IPFS CID
- 查询参数
  - `time_format` string (可选): `iso`（默认）| `unix` | `human`

- 成功响应
  - 状态码: `200 OK`
  - Body: `PinRecordView`

- 失败响应
  - `400 Bad Request`: CID 无效
  - `404 Not Found`: 找不到记录
  - `500 Internal Server Error`: 内部错误

- 示例
```bash
curl "http://localhost:8081/pins/bafy...cid...?time_format=human" --silent | jq
```

监控
- GET `/metrics`: Prometheus 指标（由服务在启动时注册）

说明
- PUT `/pins/{cid}` 会将 `{cid}` 及其 `size`（若提供）入队至 `pin.exchange`，由 Pin Worker 异步执行。
- `Active` 状态的记录在重复 `PUT` 时会刷新 TTL（通过再入队实现）。
- TTL 及到期后的 Unpin 流程由 TTLChecker 与 Unpin Worker 协作完成。


RabbitMQ 请求
以下示例展示如何直接向 RabbitMQ 发送消息以驱动 Pin 流程（绕过 HTTP）。交换机与路由键名称以默认配置为准，请根据你的 `config.yaml` 调整：

- Pin：exchange=`pin.exchange`，routing_key=`pin.queue`，payload 为 JSON：`{"cid":"<CID>", "size": <可选字节数>}`。

使用 rabbitmqadmin（推荐简单）
```bash
# Pin 示例
rabbitmqadmin publish \
  exchange=pin.exchange routing_key=pin.queue \
  payload='{"cid":"bafy...cid...","size":1048576}'

```

使用 Python（pika）
```python
import json, pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@127.0.0.1:5672/"))
ch = conn.channel()

pin_msg = json.dumps({"cid": "bafy...cid...", "size": 1048576}).encode()
ch.basic_publish(exchange="pin.exchange", routing_key="pin.queue", body=pin_msg)

# Unpin
unpin_msg = b"bafy...cid..."
ch.basic_publish(exchange="unpin.exchange", routing_key="unpin.queue", body=unpin_msg)

conn.close()
```

注意
- 交换机/队列均为 durable；服务端在启动时会自动声明拓扑。
- Pin 重试：消费失败会经 DLX 路由至重试队列（`*.retry.queue`），在 `retry_delay`（`x-message-ttl`）后回流到主交换机。
- 服务端发布时 `ContentType` 设为 `application/octet-stream`，但消费者仅按字节解析，不依赖该头部。

