# Go程序死锁检测指南

## 概述

本文档介绍如何检测和分析Go程序中的死锁问题。

## 检测方法

### 1. 使用内置的pprof工具

#### 启动程序时启用pprof
```bash
# 使用默认端口6060
./bin/ipfs_pin_service -config config.yaml

# 使用自定义端口
./bin/ipfs_pin_service -config config.yaml -pprof-port 8080
```

#### 使用死锁检测脚本
```bash
# 使用默认参数（端口6060，检测30秒）
./script/deadlock_detector.sh

# 自定义参数
./script/deadlock_detector.sh 8080 60

# 查看帮助
./script/deadlock_detector.sh -h
```

### 2. 手动使用pprof工具

#### 获取goroutine堆栈
```bash
# 获取goroutine数量
curl http://localhost:6060/debug/pprof/goroutine?debug=1 | grep -c "goroutine"

# 保存goroutine堆栈到文件
curl http://localhost:6060/debug/pprof/goroutine?debug=1 > goroutine_stack.txt

# 使用go tool pprof分析
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

#### 获取堆内存信息
```bash
# 获取堆内存使用情况
curl http://localhost:6060/debug/pprof/heap?debug=1 > heap.txt

# 使用go tool pprof分析堆内存
go tool pprof http://localhost:6060/debug/pprof/heap
```

### 3. 使用go tool trace

#### 生成trace文件
```bash
# 获取30秒的trace数据
curl http://localhost:6060/debug/pprof/trace?seconds=30 > trace.out

# 分析trace文件
go tool trace trace.out
```

## 死锁检测指标

### 1. Goroutine数量异常增长
- 正常情况下goroutine数量应该相对稳定
- 如果goroutine数量持续增长，可能存在：
  - 死锁导致goroutine无法退出
  - Goroutine泄漏
  - 资源竞争

### 2. 锁等待时间过长
- 使用pprof查看mutex profile
- 检查是否有goroutine长时间等待锁

### 3. Channel阻塞
- 检查是否有goroutine在channel操作上阻塞
- 查看channel的发送/接收操作

## 常见死锁场景

### 1. 锁的顺序问题
```go
// 错误示例：可能导致死锁
func deadlockExample() {
    var mu1, mu2 sync.Mutex
    
    go func() {
        mu1.Lock()
        time.Sleep(time.Millisecond)
        mu2.Lock()  // 等待mu2
        mu2.Unlock()
        mu1.Unlock()
    }()
    
    go func() {
        mu2.Lock()
        time.Sleep(time.Millisecond)
        mu1.Lock()  // 等待mu1
        mu1.Unlock()
        mu2.Unlock()
    }()
}
```

### 2. Channel阻塞
```go
// 错误示例：channel阻塞
func channelDeadlock() {
    ch := make(chan int)
    
    go func() {
        ch <- 1  // 发送，但没有接收者
    }()
    
    // 主goroutine没有接收，导致发送阻塞
}
```

### 3. WaitGroup使用错误
```go
// 错误示例：WaitGroup死锁
func waitGroupDeadlock() {
    var wg sync.WaitGroup
    
    wg.Add(1)
    go func() {
        defer wg.Done()
        // 某些操作
    }()
    
    wg.Wait()  // 等待，但如果goroutine没有正确调用Done()会死锁
}
```

## 分析工具

### 1. go tool pprof
```bash
# 交互式分析
go tool pprof http://localhost:6060/debug/pprof/goroutine

# 常用命令
(pprof) top          # 显示最耗时的函数
(pprof) list <func>  # 显示特定函数的详细信息
(pprof) web          # 在浏览器中显示图形化界面
(pprof) traces       # 显示调用栈
```

### 2. go tool trace
```bash
# 分析trace文件
go tool trace trace.out

# 在浏览器中查看：
# - Goroutine分析
# - 系统调用
# - 网络事件
# - 同步事件
```

### 3. 第三方工具

#### delve调试器
```bash
# 安装delve
go install github.com/go-delve/delve/cmd/dlv@latest

# 调试程序
dlv debug ./cmd/ipfs_pin_service
```

#### go-deadlock检测器
```go
import "github.com/sasha-s/go-deadlock"

// 替换sync.Mutex
var mu go-deadlock.Mutex

// 启用死锁检测
go-deadlock.Opts.DeadlockTimeout = time.Second * 30
```

## 预防措施

### 1. 代码审查
- 检查锁的获取顺序
- 确保锁的释放
- 检查channel的使用

### 2. 测试
- 编写并发测试
- 使用race detector
- 压力测试

### 3. 监控
- 监控goroutine数量
- 监控锁等待时间
- 监控channel使用情况

## 示例：分析你的RabbitMQ实现

### 检查点
1. **连接管理器**：检查连接重建逻辑是否有死锁
2. **通道管理器**：检查通道创建/关闭是否有竞争
3. **消息发布**：检查mustPublish的无限循环
4. **消费者**：检查runConsumer的通道管理

### 检测命令
```bash
# 启动程序
./bin/ipfs_pin_service -config config.yaml

# 在另一个终端运行死锁检测
./script/deadlock_detector.sh 6060 60

# 手动检查goroutine
curl http://localhost:6060/debug/pprof/goroutine?debug=1 | head -20
```

## 总结

死锁检测需要结合多种工具和方法：
1. 使用pprof监控goroutine状态
2. 使用trace分析程序执行
3. 使用race detector检测数据竞争
4. 定期进行代码审查和测试

通过持续监控和分析，可以及时发现和解决死锁问题。
