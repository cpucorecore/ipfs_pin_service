package monitor

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	OpPinAdd      = "pin_add"
	OpPinRm       = "pin_rm"
	OpRepoStat    = "repo_stat"
	OpRepoGC      = "repo_gc"
	OpBitswapStat = "bitswap_stat"
)

var (
	opDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ipfs_operation_duration_seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	opTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ipfs_operation_total",
		},
		[]string{"operation", "status"},
	)

	fileSizeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "ipfs_pin_file_size_bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
		},
	)

	// TTL buckets: count of files falling into configured size buckets
	ttlBucketCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ipfs_ttl_bucket_total",
		},
		[]string{"bucket"},
	)

	// Repo stat gauges
	repoSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ipfs_repo_size_bytes",
	})
	repoStorageMaxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ipfs_repo_storage_max_bytes",
	})
	repoNumObjects = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ipfs_repo_num_objects",
	})
	repoInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ipfs_repo_info",
	}, []string{"path", "version"})

	// Bitswap stat gauges
	bsPeers             = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_peers"})
	bsWantlist          = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_wantlist"})
	bsBlocksReceived    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_blocks_received_total"})
	bsBlocksSent        = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_blocks_sent_total"})
	bsDataReceived      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_data_received_bytes_total"})
	bsDataSent          = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_data_sent_bytes_total"})
	bsDupBlocksReceived = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_dup_blocks_received_total"})
	bsDupDataReceived   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_dup_data_received_bytes_total"})
	bsMessagesReceived  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_messages_received_total"})

	// Queue gauges
	queueReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ipfs_queue_ready"}, []string{"queue"})
	queueTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ipfs_queue_total"}, []string{"queue"})

	// IPFS availability
	ipfsAvailable = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_available"})
)

func init() {
	prometheus.MustRegister(
		opDuration,
		opTotal,
		fileSizeHist,
		ttlBucketCounter,
		repoSizeBytes,
		repoStorageMaxBytes,
		repoNumObjects,
		repoInfo,
		bsPeers,
		bsWantlist,
		bsBlocksReceived,
		bsBlocksSent,
		bsDataReceived,
		bsDataSent,
		bsDupBlocksReceived,
		bsDupDataReceived,
		bsMessagesReceived,
	)
}

// ObserveOperation records duration and success status for an IPFS operation.
func ObserveOperation(operation string, duration time.Duration, err error) {
	opDuration.WithLabelValues(operation).Observe(duration.Seconds())
	status := "success"
	if err != nil {
		status = "error"
	}
	opTotal.WithLabelValues(operation, status).Inc()
}

// ObserveFileSize records observed file size (on successful pin).
func ObserveFileSize(sizeBytes int64) {
	if sizeBytes > 0 {
		fileSizeHist.Observe(float64(sizeBytes))
	}
}

// RecordRepoStat exports repo stat metrics. Only call on success.
func RecordRepoStat(repoSize, storageMax, numObjects int64, path, version string) {
	repoSizeBytes.Set(float64(repoSize))
	repoStorageMaxBytes.Set(float64(storageMax))
	repoNumObjects.Set(float64(numObjects))
	repoInfo.WithLabelValues(path, version).Set(1)
}

// RecordBitswapStat exports bitswap stat metrics. Only call on success.
func RecordBitswapStat(
	peers, wantlist int,
	blocksReceived, blocksSent, dataReceived, dataSent, dupBlocksReceived, dupDataReceived, messagesReceived uint64,
) {
	bsPeers.Set(float64(peers))
	bsWantlist.Set(float64(wantlist))
	bsBlocksReceived.Set(float64(blocksReceived))
	bsBlocksSent.Set(float64(blocksSent))
	bsDataReceived.Set(float64(dataReceived))
	bsDataSent.Set(float64(dataSent))
	bsDupBlocksReceived.Set(float64(dupBlocksReceived))
	bsDupDataReceived.Set(float64(dupDataReceived))
	bsMessagesReceived.Set(float64(messagesReceived))
}

// RegisterMetricsRoute exposes Prometheus metrics at /metrics on the given gin router.
func RegisterMetricsRoute(r *gin.Engine) {
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

// SetQueueStats sets queue gauges for a named queue.
func SetQueueStats(queueName string, ready, total int64) {
	queueReady.WithLabelValues(queueName).Set(float64(ready))
	queueTotal.WithLabelValues(queueName).Set(float64(total))
}

// SetIPFSAvailable sets IPFS availability gauge to 1 (up) or 0 (down).
func SetIPFSAvailable(up bool) {
	if up {
		ipfsAvailable.Set(1)
	} else {
		ipfsAvailable.Set(0)
	}
}

// ObserveTTLBucket increments the counter for a given policy size bucket label.
func ObserveTTLBucket(bucket string) {
	if bucket == "" {
		bucket = "unknown"
	}
	ttlBucketCounter.WithLabelValues(bucket).Inc()
}
