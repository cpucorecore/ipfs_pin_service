#!/bin/bash

# 单独启动 RabbitMQ 脚本

set -e

echo "🐰 启动 RabbitMQ 服务..."

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行，请先启动 Docker"
    exit 1
fi

# 检查 docker-compose 是否可用
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ docker-compose 未安装，请先安装 docker-compose"
    exit 1
fi

# 启动 RabbitMQ
echo "🚀 启动 RabbitMQ 容器..."
if command -v docker-compose &> /dev/null; then
    docker-compose -f docker-compose-rabbitmq.yml up -d
else
    docker compose -f docker-compose-rabbitmq.yml up -d
fi

# 等待服务启动
echo "⏳ 等待 RabbitMQ 启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
if command -v docker-compose &> /dev/null; then
    docker-compose -f docker-compose-rabbitmq.yml ps
else
    docker compose -f docker-compose-rabbitmq.yml ps
fi

echo ""
echo "✅ RabbitMQ 启动完成！"
echo ""
echo "📊 访问信息："
echo "  - AMQP 端口: localhost:5672"
echo "  - 管理界面: http://localhost:15672"
echo "  - 用户名: admin"
echo "  - 密码: admin123"
echo ""
echo "📝 日志查看："
if command -v docker-compose &> /dev/null; then
    echo "  - docker-compose -f docker-compose-rabbitmq.yml logs -f rabbitmq"
else
    echo "  - docker compose -f docker-compose-rabbitmq.yml logs -f rabbitmq"
fi
echo ""
echo "🛑 停止服务："
if command -v docker-compose &> /dev/null; then
    echo "  - docker-compose -f docker-compose-rabbitmq.yml down"
else
    echo "  - docker compose -f docker-compose-rabbitmq.yml down"
fi
echo ""
echo "🧹 清理数据："
if command -v docker-compose &> /dev/null; then
    echo "  - docker-compose -f docker-compose-rabbitmq.yml down -v"
else
    echo "  - docker compose -f docker-compose-rabbitmq.yml down -v"
fi
