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
	OpDuration = prometheus.NewHistogramVec(
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
			Buckets: prometheus.ExponentialBuckets(1024*200, 2, 20),
		},
	)

	ttlBucketCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ipfs_ttl_bucket_total",
		},
		[]string{"bucket"},
	)

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

	bsPeers             = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_peers"})
	bsWantlist          = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_wantlist"})
	bsBlocksReceived    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_blocks_received_total"})
	bsBlocksSent        = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_blocks_sent_total"})
	bsDataReceived      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_data_received_bytes_total"})
	bsDataSent          = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_data_sent_bytes_total"})
	bsDupBlocksReceived = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_dup_blocks_received_total"})
	bsDupDataReceived   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_dup_data_received_bytes_total"})
	bsMessagesReceived  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_bitswap_messages_received_total"})

	queueReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ipfs_queue_ready"}, []string{"queue"})
	queueTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ipfs_queue_total"}, []string{"queue"})

	ipfsAvailable = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_available"})

	filterSizeLimitGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ipfs_filter_size_limit_bytes"})
	filterTotal          = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "ipfs_filter_total"},
		[]string{"result"},
	)
	filterRequestSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "ipfs_filter_request_size_bytes", Buckets: prometheus.ExponentialBuckets(1024, 2, 20)},
	)
)

func init() {
	prometheus.MustRegister(
		OpDuration,
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
		filterSizeLimitGauge,
		filterTotal,
		filterRequestSize,
	)
}

func ObserveOperation(operation string, duration time.Duration, err error) {
	OpDuration.WithLabelValues(operation).Observe(duration.Seconds())
	status := "success"
	if err != nil {
		status = "error"
	}
	opTotal.WithLabelValues(operation, status).Inc()
}

func ObserveFileSize(sizeBytes int64) {
	if sizeBytes > 0 {
		fileSizeHist.Observe(float64(sizeBytes))
	}
}

func RecordRepoStat(repoSize, storageMax, numObjects int64, path, version string) {
	repoSizeBytes.Set(float64(repoSize))
	repoStorageMaxBytes.Set(float64(storageMax))
	repoNumObjects.Set(float64(numObjects))
	repoInfo.WithLabelValues(path, version).Set(1)
}

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

func RegisterMetricsRoute(r *gin.Engine) {
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

func SetQueueStats(queueName string, ready, total int64) {
	queueReady.WithLabelValues(queueName).Set(float64(ready))
	queueTotal.WithLabelValues(queueName).Set(float64(total))
}

func SetIPFSAvailable(up bool) {
	if up {
		ipfsAvailable.Set(1)
	} else {
		ipfsAvailable.Set(0)
	}
}

func ObserveTTLBucket(bucket string) {
	if bucket == "" {
		bucket = "unknown"
	}
	ttlBucketCounter.WithLabelValues(bucket).Inc()
}

func SetFilterSizeLimit(limit int64) {
	if limit < 0 {
		limit = 0
	}
	filterSizeLimitGauge.Set(float64(limit))
}

func ObserveFilter(size int64, filtered bool) {
	if size > 0 {
		filterRequestSize.Observe(float64(size))
	}
	result := "accepted"
	if filtered {
		result = "filtered"
	}
	filterTotal.WithLabelValues(result).Inc()
}
