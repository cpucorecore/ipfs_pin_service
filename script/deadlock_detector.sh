#!/bin/bash

# 死锁检测脚本
# 使用方法: ./script/deadlock_detector.sh <pprof_port> <duration_seconds>

set -e

PPROF_PORT=${1:-6060}
DURATION=${2:-30}
INTERVAL=5

echo "🔍 开始死锁检测..."
echo "📊 pprof端口: $PPROF_PORT"
echo "⏱️  检测时长: ${DURATION}秒"
echo "🔄 检测间隔: ${INTERVAL}秒"
echo ""

# 检查pprof端口是否可访问
check_pprof_available() {
    if ! curl -s "http://localhost:$PPROF_PORT/debug/pprof/" > /dev/null 2>&1; then
        echo "❌ 无法连接到pprof端口 $PPROF_PORT"
        echo "请确保程序已启动并启用了pprof"
        exit 1
    fi
}

# 获取goroutine数量
get_goroutine_count() {
    curl -s "http://localhost:$PPROF_PORT/debug/pprof/goroutine?debug=1" | grep -c "goroutine" || echo "0"
}

# 获取goroutine堆栈
get_goroutine_stack() {
    curl -s "http://localhost:$PPROF_PORT/debug/pprof/goroutine?debug=1"
}

# 检测死锁
detect_deadlock() {
    local count=$1
    local prev_count=$2
    
    echo "📈 当前goroutine数量: $count"
    
    # 如果goroutine数量持续增长，可能是死锁
    if [ "$count" -gt "$prev_count" ]; then
        echo "⚠️  goroutine数量增加: $prev_count -> $count"
    fi
    
    # 如果goroutine数量超过1000，发出警告
    if [ "$count" -gt 1000 ]; then
        echo "🚨 警告: goroutine数量过多 ($count)，可能存在死锁或goroutine泄漏"
        return 1
    fi
    
    return 0
}

# 保存goroutine堆栈到文件
save_goroutine_stack() {
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local filename="goroutine_stack_${timestamp}.txt"
    
    echo "💾 保存goroutine堆栈到: $filename"
    get_goroutine_stack > "$filename"
    echo "📄 文件大小: $(du -h "$filename" | cut -f1)"
}

# 主检测循环
main() {
    check_pprof_available
    echo "✅ pprof连接正常"
    echo ""
    
    local start_time=$(date +%s)
    local prev_count=0
    local deadlock_detected=false
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $DURATION ]; then
            break
        fi
        
        echo "⏰ $(date '+%H:%M:%S') - 检测中... ($elapsed/$DURATION秒)"
        
        local count=$(get_goroutine_count)
        
        if ! detect_deadlock $count $prev_count; then
            deadlock_detected=true
            echo "🚨 检测到潜在死锁！"
            save_goroutine_stack
            break
        fi
        
        prev_count=$count
        sleep $INTERVAL
    done
    
    echo ""
    echo "🏁 检测完成"
    
    if [ "$deadlock_detected" = true ]; then
        echo "❌ 检测到潜在死锁问题"
        echo "📋 建议检查:"
        echo "  1. 查看保存的goroutine堆栈文件"
        echo "  2. 检查锁的使用是否正确"
        echo "  3. 检查channel操作是否有阻塞"
        echo "  4. 使用 'go tool pprof' 进行详细分析"
        exit 1
    else
        echo "✅ 未检测到明显的死锁问题"
        echo "📊 最终goroutine数量: $prev_count"
    fi
}

# 显示帮助信息
show_help() {
    echo "使用方法: $0 [pprof_port] [duration_seconds]"
    echo ""
    echo "参数:"
    echo "  pprof_port        pprof服务器端口 (默认: 6060)"
    echo "  duration_seconds  检测时长，单位秒 (默认: 30)"
    echo ""
    echo "示例:"
    echo "  $0 6060 60        # 检测60秒，使用6060端口"
    echo "  $0 8080           # 检测30秒，使用8080端口"
    echo "  $0                # 使用默认参数"
}

# 检查参数
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

# 运行主程序
main
