# IPFS Pin Service Docker Compose 部署

这个目录包含了用于部署 IPFS Pin Service 相关服务的 Docker Compose 配置。

## 服务组件

- **RabbitMQ**: 消息队列服务 (端口: 5672, 管理界面: 15672)
- **Prometheus**: 监控数据收集服务 (端口: 9090)
- **Grafana**: 监控数据可视化服务 (端口: 3000)

## 快速开始

### 1. 启动服务

```bash
cd docker-compose
docker-compose up -d
```

### 2. 访问服务

- **RabbitMQ 管理界面**: http://localhost:15672
  - 用户名: `admin`
  - 密码: `admin123`

- **Prometheus**: http://localhost:9090

- **Grafana**: http://localhost:3000
  - 用户名: `admin`
  - 密码: `admin123`

### 3. 配置 IPFS Pin Service

确保你的 IPFS Pin Service 配置文件中包含以下设置：

```yaml
# config.yaml
rabbitmq:
  url: "amqp://admin:admin123@localhost:5672/"

monitor:
  enabled: true
  port: 8081  # 确保这个端口与 Prometheus 配置中的端口一致
```

## 监控指标

Prometheus 会自动收集以下指标：

- `ipfs_pin_requests_total`: Pin 请求总数
- `ipfs_pin_request_duration_seconds`: Pin 请求耗时
- `ipfs_file_size_bytes`: 文件大小分布
- `ipfs_filter_total`: 过滤操作统计
- `ipfs_filter_size_limit_bytes`: 过滤大小限制
- `ipfs_pin_operations_total`: Pin 操作统计

## Grafana 仪表板

启动后，Grafana 会自动加载预配置的仪表板，包括：

- Pin 请求速率
- 请求响应时间 (95th percentile)
- 文件大小分布
- 过滤操作统计
- 过滤大小限制
- Pin 操作速率

## 故障排除

### 1. 端口冲突

如果端口被占用，可以修改 `docker-compose.yml` 中的端口映射：

```yaml
ports:
  - "9091:9090"  # 将 Prometheus 端口改为 9091
```

### 2. 网络连接问题

确保 Docker 容器能够访问宿主机服务：

- 在 macOS/Windows 上，使用 `host.docker.internal` 访问宿主机
- 在 Linux 上，可能需要使用 `host.docker.internal` 或 `172.17.0.1`

### 3. 权限问题

如果遇到权限问题，确保 Docker 有足够的权限访问挂载的目录。

## 停止服务

```bash
docker-compose down
```

## 清理数据

```bash
docker-compose down -v  # 删除数据卷
```

## 自定义配置

### 修改 Prometheus 配置

编辑 `prometheus/prometheus.yml` 文件来修改监控目标或规则。

### 修改 Grafana 仪表板

1. 在 Grafana 中编辑仪表板
2. 导出 JSON 配置
3. 替换 `grafana/dashboards/ipfs-pin-service-dashboard.json`

### 添加告警规则

在 `prometheus/prometheus.yml` 中添加 `rule_files` 配置，并创建相应的告警规则文件。
