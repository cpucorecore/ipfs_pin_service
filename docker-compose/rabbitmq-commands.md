# RabbitMQ Docker 快速命令参考

## 启动 RabbitMQ

### 使用脚本启动
```bash
chmod +x start-rabbitmq.sh
./start-rabbitmq.sh
```

### 手动启动
```bash
# 新版本 docker compose
docker compose -f docker-compose-rabbitmq.yml up -d

# 传统版本 docker-compose
docker-compose -f docker-compose-rabbitmq.yml up -d
```

## 查看状态

```bash
# 新版本
docker compose -f docker-compose-rabbitmq.yml ps

# 传统版本
docker-compose -f docker-compose-rabbitmq.yml ps
```

## 查看日志

```bash
# 新版本
docker compose -f docker-compose-rabbitmq.yml logs -f rabbitmq

# 传统版本
docker-compose -f docker-compose-rabbitmq.yml logs -f rabbitmq
```

## 停止服务

```bash
# 新版本
docker compose -f docker-compose-rabbitmq.yml down

# 传统版本
docker-compose -f docker-compose-rabbitmq.yml down
```

## 清理数据

```bash
# 新版本
docker compose -f docker-compose-rabbitmq.yml down -v

# 传统版本
docker-compose -f docker-compose-rabbitmq.yml down -v
```

## 访问信息

- **AMQP 端口**: localhost:5672
- **管理界面**: http://localhost:15672
- **用户名**: admin
- **密码**: admin123

## 配置说明

### 环境变量
- `RABBITMQ_DEFAULT_USER=admin`: 默认用户名
- `RABBITMQ_DEFAULT_PASS=admin123`: 默认密码
- `RABBITMQ_DEFAULT_VHOST=/`: 默认虚拟主机

### 端口映射
- `5672:5672`: AMQP 协议端口
- `15672:15672`: 管理界面端口

### 数据持久化
- 数据存储在 `rabbitmq_data` 卷中
- 重启容器后数据不会丢失

## 健康检查

容器包含健康检查，每30秒检查一次 RabbitMQ 服务状态。

## 故障排除

### 端口被占用
如果端口被占用，可以修改 `docker-compose-rabbitmq.yml` 中的端口映射：

```yaml
ports:
  - "5673:5672"   # 将 AMQP 端口改为 5673
  - "15673:15672" # 将管理界面端口改为 15673
```

### 权限问题
确保 Docker 有足够的权限访问挂载的目录。

### 连接问题
如果 IPFS Pin Service 无法连接，检查：
1. RabbitMQ 是否正常运行
2. 端口是否正确
3. 用户名密码是否正确
