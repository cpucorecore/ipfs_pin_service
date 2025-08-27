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
	"sync"
	"syscall"

	"github.com/cpucorecore/ipfs_pin_service/internal/api"
	"github.com/cpucorecore/ipfs_pin_service/internal/config"
	"github.com/cpucorecore/ipfs_pin_service/internal/filter"
	"github.com/cpucorecore/ipfs_pin_service/internal/ipfs"
	"github.com/cpucorecore/ipfs_pin_service/internal/monitor"
	"github.com/cpucorecore/ipfs_pin_service/internal/queue"
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

	if *showVersion {
		fmt.Println(GetVersion())
		os.Exit(0)
	}

	log.InitLoggerForTest()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to load config: %v", err)
	}

	log.InitLoggerWithConfig(cfg)

	go func() {
		pprofAddr := fmt.Sprintf(":%d", *pprofPort)
		log.Log.Sugar().Infof("Starting pprof server on %s", pprofAddr)
		if err = http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Log.Sugar().Errorf("pprof server error: %v", err)
		}
	}()

	st, err := store.NewPebbleStore(".db")
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to create store: %v", err)
	}
	defer st.Close()

	mq, err := queue.NewRabbitMQ(cfg)
	if err != nil {
		log.Log.Sugar().Fatalf("Failed to create queue: %v", err)
	}
	defer mq.Close()

	f := filter.New(cfg)
	server := api.NewServer(st, mq, f)
	router := gin.Default()
	server.Routes(router)
	monitor.RegisterMetricsRoute(router)

	policy := ttl.NewPolicy(cfg)
	ipfsClient := ipfs.NewClientWithConfig(cfg.IPFS.APIAddr, cfg)
	pinWorker := worker.NewPinWorker(st, mq, ipfsClient, policy, cfg)
	unpinWorker := worker.NewUnpinWorker(st, mq, ipfsClient, cfg)
	gcWorker := worker.NewGCWorker(ipfsClient, cfg)
	statWorker := worker.NewStatWorker(ipfsClient, cfg)
	bitswapStatWorker := worker.NewBitswapStatWorker(ipfsClient, cfg)
	queueMonitor := worker.NewQueueMonitor(mq, cfg)
	ttlChecker := worker.NewTTLChecker(st, mq, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = pinWorker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("Pin worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = unpinWorker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("Unpin worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = gcWorker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("GC worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = ttlChecker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("TTL checker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = statWorker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("Stat worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = bitswapStatWorker.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("Bitswap stat worker stopped: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = queueMonitor.Start(ctx); err != nil {
			log.Log.Sugar().Errorf("Queue monitor stopped: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler: router,
	}

	go func() {
		if err = srv.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
			log.Log.Sugar().Errorf("HTTP server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	cancel()
	if err = srv.Shutdown(context.Background()); err != nil {
		log.Log.Sugar().Errorf("HTTP server shutdown error: %v", err)
	}
	wg.Wait()
}
