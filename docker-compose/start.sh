#!/bin/bash

# IPFS Pin Service Docker Compose 启动脚本

set -e

echo "🚀 启动 IPFS Pin Service 监控栈..."

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行，请先启动 Docker"
    exit 1
fi

# 检查 docker-compose 是否可用
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose 未安装，请先安装 docker-compose"
    exit 1
fi

# 创建必要的目录
echo "📁 创建必要的目录..."
mkdir -p prometheus grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards

# 启动服务
echo "🐳 启动 Docker 服务..."
docker-compose up -d

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
docker-compose ps

echo ""
echo "✅ 服务启动完成！"
echo ""
echo "📊 访问地址："
echo "  - RabbitMQ 管理界面: http://localhost:15672 (admin/admin123)"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3000 (admin/admin123)"
echo ""
echo "📝 日志查看："
echo "  - docker-compose logs -f rabbitmq"
echo "  - docker-compose logs -f prometheus"
echo "  - docker-compose logs -f grafana"
echo ""
echo "🛑 停止服务："
echo "  - docker-compose down"
