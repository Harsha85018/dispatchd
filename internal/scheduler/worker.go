package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/store"
)

// WorkerPool pulls ready jobs off a channel and executes them
// using handlers from the registry, persisting the outcome.
type WorkerPool struct {
	store    *store.Store
	registry *job.Registry
	ready    <-chan *job.Job
	sched    *Scheduler
	size     int
	wg       sync.WaitGroup
}

func NewWorkerPool(s *store.Store, r *job.Registry, sched *Scheduler, size int) *WorkerPool {
	return &WorkerPool{
		store:    s,
		registry: r,
		ready:    sched.Ready,
		sched:    sched,
		size:     size,
	}
}

// Start launches the worker goroutines. Call Wait to block until
// the ready channel is closed and all in-flight jobs finish.
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.size; i++ {
		wp.wg.Add(1)
		go wp.run(i)
	}
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (wp *WorkerPool) run(id int) {
	defer wp.wg.Done()

	for j := range wp.ready {
		handler, ok := wp.registry.Get(j.Type)
		if !ok {
			log.Printf("worker %d: no handler registered for type %q (job %s)", id, j.Type, j.ID)
			wp.finish(j, job.StatusFailed, "no handler registered for job type")
			continue
		}

		j.Attempts++
		err := handler(j)

		if err == nil {
			log.Printf("worker %d: job %s succeeded (attempt %d)", id, j.ID, j.Attempts)
			wp.finish(j, job.StatusSucceeded, "")
			continue
		}

		if j.Attempts < j.MaxAttempts {
			log.Printf("worker %d: job %s failed (attempt %d/%d), will retry: %v",
				id, j.ID, j.Attempts, j.MaxAttempts, err)
			// back to pending so the scheduler picks it up again
			wp.finish(j, job.StatusPending, err.Error())
		} else {
			log.Printf("worker %d: job %s failed permanently after %d attempts: %v",
				id, j.ID, j.Attempts, err)
			wp.finish(j, job.StatusFailed, err.Error())
		}
	}
}

func (wp *WorkerPool) finish(j *job.Job, status job.Status, errMsg string) {
	j.Status = status
	j.Error = errMsg
	j.UpdatedAt = time.Now().UTC()
	if err := wp.store.Put(j); err != nil {
		log.Printf("worker: failed to persist job %s: %v", j.ID, err)
	}
	wp.sched.Done(j.ID)
}