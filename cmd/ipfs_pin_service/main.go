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
	"github.com/cpucorecore/ipfs_pin_service/internal/mq"
	"github.com/cpucorecore/ipfs_pin_service/internal/shutdown"
	"github.com/cpucorecore/ipfs_pin_service/internal/store"
	"github.com/cpucorecore/ipfs_pin_service/internal/ttl"
	"github.com/cpucorecore/ipfs_pin_service/internal/worker"
	"github.com/cpucorecore/ipfs_pin_service/log"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
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
		log.Log.Fatal("Failed to create store", zap.Error(err))
	}
	defer pebbleStore.Close()

	shutdownMgr := shutdown.NewManager()
	mq, err := mq.NewGoRabbitMQ(cfg.RabbitMQ)
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to create queue: %v", err)
	}
	defer mq.Close()

	sizeFilter := filter.NewSizeFilter(cfg.Filter.SizeLimit.Int64())
	apiServer := api.NewServer(pebbleStore, mq, sizeFilter, shutdownMgr)
	ginEngine := gin.Default()
	apiServer.RegisterHandles(ginEngine)
	monitor.RegisterHandles(ginEngine)

	policy := ttl.NewPolicy(cfg.TTL)
	ipfsClient := ipfs.NewClientWithConfig(cfg.IPFS.APIAddr, cfg.IPFS)
	pinWorker := worker.NewPinWorker(cfg.PinWorker, pebbleStore, mq, ipfsClient, policy)
	unpinWorker := worker.NewUnpinWorker(cfg.UnpinWorker, pebbleStore, mq, ipfsClient)
	provideWorker := worker.NewProvideWorker(cfg.ProvideWorker, cfg.ProvideRecursive, pebbleStore, mq, ipfsClient)
	gcWorker := worker.NewGCWorker(cfg.GC.Interval, ipfsClient, shutdownMgr)
	statWorker := worker.NewStatWorker(ipfsClient, shutdownMgr)
	bitswapStatWorker := worker.NewBitswapStatWorker(ipfsClient, shutdownMgr)
	queueMonitor := worker.NewQueueMonitor(mq, shutdownMgr)
	ttlChecker := worker.NewTTLChecker(cfg.TTLChecker, pebbleStore, mq, shutdownMgr)

	shutdownMgr.Go("pinWorker", func() {
		pinWorker.Start()
	})

	shutdownMgr.Go("unpinWorker", func() {
		unpinWorker.Start()
	})

	shutdownMgr.Go("provideWorker", func() {
		provideWorker.Start()
	})

	shutdownMgr.Go("gcWorker", func() {
		if err = gcWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("GC worker stopped: %v", err)
		}
	})

	shutdownMgr.Go("ttlChecker", func() {
		if err = ttlChecker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("TTL checker stopped: %v", err)
		}
	})

	shutdownMgr.Go("statWorker", func() {
		if err = statWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Stat worker stopped: %v", err)
		}
	})

	shutdownMgr.Go("bitswapStatWorker", func() {
		if err = bitswapStatWorker.Start(shutdownMgr.ShutdownCtx()); err != nil {
			log.Log.Sugar().Errorf("Bitswap stat worker stopped: %v", err)
		}
	})

	shutdownMgr.Go("queueMonitor", func() {
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

	log.Log.Sugar().Info("Closing RabbitMQ connection...")
	if err = mq.Close(); err != nil {
		log.Log.Sugar().Errorf("RabbitMQ close error: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err = httpServer.Shutdown(shutdownCtx); err != nil {
		log.Log.Sugar().Errorf("HTTP server shutdown error: %v", err)
	}

	shutdownMgr.WaitForCompletion()
	log.Log.Sugar().Info("Shutdown complete")

	os.Exit(0)
}
