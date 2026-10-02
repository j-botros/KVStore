package controller

import (
	"net/http"

	storageengine "kvstore/pkg/kvstore/storageEngine"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MonitoringController struct {
	engine *storageengine.StorageEngine

	// Gauges — refreshed on each scrape
	keysTotal         prometheus.Gauge
	walSizeBytes      prometheus.Gauge
	sstableDiskUsage  prometheus.Gauge
	memtableSizeBytes prometheus.Gauge
}

var (
	// --- Counters (package-level, shared with StoreController) ---
	GetRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kv_get_requests_total",
		Help: "Total number of GET requests processed.",
	})
	PutRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kv_put_requests_total",
		Help: "Total number of PUT requests processed.",
	})
	DeleteRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kv_delete_requests_total",
		Help: "Total number of DELETE requests processed.",
	})

	// --- Histograms ---
	GetLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kv_get_latency_ms",
		Help:    "Latency of GET requests in milliseconds.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	})
	PutLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kv_put_latency_ms",
		Help:    "Latency of PUT requests in milliseconds.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	})
	DeleteLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kv_delete_latency_ms",
		Help:    "Latency of DELETE requests in milliseconds.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	})
)

func NewMonitoringController(engine *storageengine.StorageEngine) *MonitoringController {
	mc := &MonitoringController{
		engine: engine,
		keysTotal: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kv_keys_total",
			Help: "Current estimated number of live keys in the store.",
		}),
		walSizeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kv_wal_size_bytes",
			Help: "Total size in bytes of all WAL files on disk.",
		}),
		sstableDiskUsage: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kv_sstable_disk_usage_bytes",
			Help: "Total disk usage in bytes of all SSTable files.",
		}),
		memtableSizeBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kv_memtable_size_bytes",
			Help: "Current size in bytes of the active memtable (including immutables).",
		}),
	}

	// Register storage-layer counters using CounterFunc to dynamically read from the engine
	promauto.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "kv_compaction_runs_total",
			Help: "Total number of compaction runs completed.",
		},
		func() float64 {
			return float64(engine.Stats().CompactionRunsTotal)
		},
	)

	promauto.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "kv_flush_runs_total",
			Help: "Total number of memtable flush runs completed.",
		},
		func() float64 {
			return float64(engine.Stats().FlushRunsTotal)
		},
	)

	return mc
}

// RegisterRoutes mounts the /metrics endpoint.
func (mc *MonitoringController) RegisterRoutes(mux *http.ServeMux) {
	// Wrap the default handler with a before-scrape refresh of all Gauge metrics.
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mc.refreshGauges()
		promhttp.Handler().ServeHTTP(w, r)
	}))
}

// refreshGauges reads a Stats() snapshot from the engine and updates all Gauge metrics.
func (mc *MonitoringController) refreshGauges() {
	stats := mc.engine.Stats()
	mc.keysTotal.Set(float64(stats.KeysTotal))
	mc.walSizeBytes.Set(float64(stats.WalSizeBytes))
	mc.sstableDiskUsage.Set(float64(stats.SstableDiskUsageBytes))
	mc.memtableSizeBytes.Set(float64(stats.MemtableSizeBytes))
}