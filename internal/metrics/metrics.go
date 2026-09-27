// Package metrics defines the Prometheus collectors dispatchd exposes on
// /metrics. Collectors are registered at init via promauto, so importing
// this package is enough to make them visible.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatchd_http_requests_total",
		Help: "HTTP requests handled, by endpoint and response status.",
	}, []string{"path", "status"})

	// Buckets run from 0.5ms to roughly 4s. Lease and complete calls are
	// sub-millisecond when healthy, so the low end needs the resolution.
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dispatchd_http_request_duration_seconds",
		Help:    "Request latency by endpoint.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"path"})

	JobsSubmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatchd_jobs_submitted_total",
		Help: "Jobs accepted through the API.",
	})

	JobsLeased = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatchd_jobs_leased_total",
		Help: "Leases handed to workers. Exceeds jobs submitted when jobs retry.",
	})

	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatchd_jobs_completed_total",
		Help: "Job attempts reported by workers, by outcome.",
	}, []string{"outcome"})

	LeasesRenewed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatchd_leases_renewed_total",
		Help: "Heartbeat renewals accepted.",
	})

	LeasesLost = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatchd_leases_lost_total",
		Help: "Renew or complete calls rejected because the lease was gone.",
	})

	LeasesReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dispatchd_leases_reaped_total",
		Help: "Expired leases requeued. Non-zero means workers are dying or overrunning their leases.",
	})

	JobsByStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dispatchd_jobs",
		Help: "Jobs currently in each status, sampled from the store.",
	}, []string{"status"})
)

// Instrument wraps a handler with request counting and latency observation.
// The path label is supplied by the caller rather than read from the
// request, so /jobs/{id} stays one time series instead of becoming one per
// job id — unbounded label cardinality is the classic way to melt a
// Prometheus server.
func Instrument(path string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		h(rec, r)

		HTTPDuration.WithLabelValues(path).Observe(time.Since(start).Seconds())
		HTTPRequests.WithLabelValues(path, strconv.Itoa(rec.status)).Inc()
	}
}

// statusRecorder captures the response code so it can be used as a label.
// A handler that never calls WriteHeader has implicitly sent 200.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
