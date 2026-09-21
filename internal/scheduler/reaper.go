package scheduler

import (
	"log"
	"time"

	"github.com/Harsha85018/dispatchd/internal/store"
)

// RunReaper periodically requeues jobs whose leases have expired.
// Without this, a job held by a worker that crashed would stay
// leased forever and never run again.
func RunReaper(s *store.Store, interval time.Duration, done <-chan struct{}) {
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
			for _, id := range reaped {
				log.Printf("reaper: requeued job %s (lease expired)", id)
			}
		}
	}
}