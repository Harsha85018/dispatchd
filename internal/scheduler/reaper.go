package scheduler

import (
	"log"
	"time"

	"github.com/Harsha85018/dispatchd/internal/metrics"
)

// Reaper is the only store behavior the reaper loop needs.
type Reaper interface {
	ReapExpired() ([]string, error)
}

// RunReaper periodically requeues jobs whose leases have expired.
// Without this, a job held by a worker that crashed would stay leased
// forever and never run again.
func RunReaper(s Reaper, interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			reaped, err := s.ReapExpired()
			if err != nil {
				log.Printf("reaper: failed to reap expired leases: %v", err)
				continue
			}
			metrics.LeasesReaped.Add(float64(len(reaped)))
			for _, id := range reaped {
				log.Printf("reaper: requeued job %s (lease expired)", id)
			}
		}
	}
}

// StatusSampler is the store behavior the queue-depth gauge needs.
type StatusSampler interface {
	StatusCounts() (map[string]int, error)
}

// sampledStatuses is fixed rather than derived from the query, because a
// status with no rows returns no row at all — and a gauge left unset keeps
// its last value forever instead of dropping to zero.
var sampledStatuses = []string{"pending", "leased", "succeeded", "failed"}

// RunStatusSampler publishes how many jobs sit in each status. Queue depth
// is a level, not an event, so it can't be derived from counters; polling
// the store is cheaper than threading transition hooks through both store
// implementations.
func RunStatusSampler(s StatusSampler, interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			counts, err := s.StatusCounts()
			if err != nil {
				log.Printf("sampler: failed to read status counts: %v", err)
				continue
			}
			for _, status := range sampledStatuses {
				metrics.JobsByStatus.WithLabelValues(status).Set(float64(counts[status]))
			}
		}
	}
}
