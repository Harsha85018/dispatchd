package scheduler

import (
	"log"
	"time"
	"sync"

	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/store"
)

// Scheduler periodically scans the store for pending jobs whose
// dependencies have all succeeded, and pushes them onto the Ready channel
// for workers to pick up.
type Scheduler struct {
	store *store.Store
	Ready chan *job.Job
	poll  time.Duration

	mu       sync.Mutex
	inFlight map[string]bool // job IDs dispatched but not yet reported done
}

func NewScheduler(s *store.Store, bufferSize int, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		store:    s,
		Ready:    make(chan *job.Job, bufferSize),
		poll:     pollInterval,
		inFlight: make(map[string]bool),
	}
}

// Done marks a job as no longer in flight, so it can be dispatched again
// on a later tick (e.g. after a failed attempt returns it to pending).
func (sch *Scheduler) Done(id string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	delete(sch.inFlight, id)
}

// claim marks a job as in flight. It returns false if the job was
// already claimed, which prevents double dispatch.
func (sch *Scheduler) claim(id string) bool {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	if sch.inFlight[id] {
		return false
	}
	sch.inFlight[id] = true
	return true
}

// Run starts the scheduling loop. It blocks, so call it in a goroutine.
// Stop the loop by closing the done channel.
func (sch *Scheduler) Run(done <-chan struct{}) {
	ticker := time.NewTicker(sch.poll)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			close(sch.Ready)
			return
		case <-ticker.C:
			sch.tick()
		}
	}
}

// tick scans all jobs once and dispatches any that are ready.
func (sch *Scheduler) tick() {
	all := sch.store.All()

	// index statuses so dependency checks are O(1) per lookup
	statusByID := make(map[string]job.Status, len(all))
	for _, j := range all {
		statusByID[j.ID] = j.Status
	}

	for _, j := range all {
		if j.Status != job.StatusPending {
			continue
		}
		if sch.dependenciesSatisfied(j, statusByID) {
			if !sch.claim(j.ID) {
				continue // already dispatched, waiting on a worker
			}

			j.Status = job.StatusRunning
			j.UpdatedAt = time.Now().UTC()
			if err := sch.store.Put(j); err != nil {
				log.Printf("scheduler: failed to persist status change for %s: %v", j.ID, err)
				sch.Done(j.ID)
				continue
			}

			select {
			case sch.Ready <- j:
			default:
				j.Status = job.StatusPending
				sch.store.Put(j)
				sch.Done(j.ID)
				log.Printf("scheduler: ready channel full, deferring job %s", j.ID)
			}
		}
	}
}

// dependenciesSatisfied returns true if every job this job depends on
// has succeeded. A missing dependency (not found at all) is treated
// as unsatisfied rather than skipped, to avoid running jobs whose
// prerequisites don't exist.
func (sch *Scheduler) dependenciesSatisfied(j *job.Job, statusByID map[string]job.Status) bool {
	for _, depID := range j.DependsOn {
		status, ok := statusByID[depID]
		if !ok || status != job.StatusSucceeded {
			return false
		}
	}
	return true
}