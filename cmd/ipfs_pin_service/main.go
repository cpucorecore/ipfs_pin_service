package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cpucorecore/ipfs_pin_service/internal/api"
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/filter"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/worker"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	showVersion := flag.Bool("v", false, "Show version information")
	pprofPort := flag.Int("pprof-port", 6060, "Port for pprof debugging")
	flag.Parse()

	ShowVersion(*showVersion)

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(fmt.Sprintf("load config error: %s", err))
	}

	log.InitLoggerWithConfig(cfg)

	StartPprof(*pprofPort)

	pebbleStore, err := store.NewPebbleStore(".db")
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to create store: %v", err)
	}
	defer pebbleStore.Close()

	shutdownMgr := shutdown.NewManager()
	mq, err := queue.NewRabbitMQ(cfg, shutdownMgr)
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to create queue: %v", err)
	}
	defer mq.Close()

	sizeFilter := filter.NewSizeFilter(cfg)
	apiServer := api.NewServer(pebbleStore, mq, sizeFilter, shutdownMgr)
	ginEngine := gin.Default()
	apiServer.RegisterHandles(ginEngine)
	monitor.RegisterHandles(ginEngine)

	policy := ttl.NewPolicy(cfg)
	ipfsClient := ipfs.NewClientWithConfig(cfg.IPFS.APIAddr, cfg)
	pinWorker := worker.NewPinWorker(pebbleStore, mq, ipfsClient, policy, cfg)
	unpinWorker := worker.NewUnpinWorker(pebbleStore, mq, ipfsClient, cfg)
	provideWorker := worker.NewProvideWorker(pebbleStore, mq, ipfsClient, cfg)
	gcWorker := worker.NewGCWorker(ipfsClient, cfg, shutdownMgr)
	statWorker := worker.NewStatWorker(ipfsClient, cfg, shutdownMgr)
	bitswapStatWorker := worker.NewBitswapStatWorker(ipfsClient, cfg, shutdownMgr)
	queueMonitor := worker.NewQueueMonitor(mq, cfg, shutdownMgr)
	ttlChecker := worker.NewTTLChecker(pebbleStore, mq, cfg, shutdownMgr)

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownMgr.Go(func() {
		if err = pinWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Pin worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = unpinWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Unpin worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = provideWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Provide worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = gcWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("GC worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = ttlChecker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("TTL checker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = statWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Stat worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = bitswapStatWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Bitswap stat worker stopped: %v", err)
		}
	})

	shutdownMgr.Go(func() {
		if err = queueMonitor.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Queue monitor stopped: %v", err)
		}
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler: ginEngine,
	}

	go func() {
		if err = httpServer.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
			log.Log.Sugar().Errorf("HTTP server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Log.Sugar().Info("Received shutdown signal, entering drain mode...")

	shutdownMgr.StartDrain()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err = httpServer.Shutdown(shutdownCtx); err != nil {
		log.Log.Sugar().Errorf("HTTP server shutdown error: %v", err)
	}

	shutdownMgr.WaitForCompletion()
	log.Log.Sugar().Info("Shutdown complete")

	os.Exit(0)
}
