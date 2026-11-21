package metrics

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var CacheReads = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orderflow_cache_reads_total", Help: "Order cache lookups by result."}, []string{"result"})
var KafkaWrites = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orderflow_kafka_writes_total", Help: "Kafka publish attempts by result."}, []string{"result"})

type HTTPMetrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewHTTPMetrics() *HTTPMetrics {
	registry := prometheus.NewRegistry()
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "orderflow",
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests handled by the API.",
	}, []string{"method", "route", "status"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "orderflow",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route"})
	registry.MustRegister(requests, duration, CacheReads, KafkaWrites, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return &HTTPMetrics{registry: registry, requests: requests, duration: duration}
}

func (m *HTTPMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id [16]byte
		rand.Read(id[:])
		requestID := hex.EncodeToString(id[:])
		w.Header().Set("X-Request-ID", requestID)
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		method := r.Method
		slog.Info("http request", "request_id", requestID, "method", method, "route", route, "order_id", r.PathValue("id"), "status", recorder.status, "duration_ms", time.Since(started).Milliseconds())
		m.requests.WithLabelValues(method, route, strconv.Itoa(recorder.status)).Inc()
		m.duration.WithLabelValues(method, route).Observe(time.Since(started).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
