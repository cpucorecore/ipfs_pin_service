package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/cpucorecore/ipfs_pin_service/internal/api"
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/worker"
	"github.com/gin-gonic/gin"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 创建存储
	st, err := store.NewPebbleStore(".db")
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer st.Close()

	// 创建消息队列
	mq, err := queue.NewRabbitMQ(cfg)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer mq.Close()

	// 创建 IPFS 客户端
	ipfsClient := ipfs.NewClient(cfg.IPFS.APIAddr)

	// 创建 TTL 策略
	policy := ttl.NewPolicy(cfg)

	// 创建 HTTP 服务器
	server := api.NewServer(st, mq)
	router := gin.Default()
	server.Routes(router)

	// 创建 workers
	pinWorker := worker.NewPinWorker(st, mq, ipfsClient, policy, cfg)
	unpinWorker := worker.NewUnpinWorker(st, mq, ipfsClient, cfg)
	gcWorker := worker.NewGCWorker(ipfsClient, cfg)
	ttlChecker := worker.NewTTLChecker(st, mq, cfg)

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动 workers
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = pinWorker.Start(ctx); err != nil {
			log.Printf("Pin worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := unpinWorker.Start(ctx); err != nil {
			log.Printf("Unpin worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := gcWorker.Start(ctx); err != nil {
			log.Printf("GC worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ttlChecker.Start(ctx); err != nil {
			log.Printf("TTL checker stopped: %v", err)
		}
	}()

	// 启动 HTTP 服务器
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 优雅关闭
	cancel()
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	wg.Wait()
}
