package main

import (
	"log"
	"time"

	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/scheduler"
	"github.com/Harsha85018/dispatchd/internal/store"
)

func main() {
	s, err := store.NewStore("dispatchd.wal")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	// register handlers for the job types we'll use in the demo
	registry := job.NewRegistry()
	registry.Register("extract", job.SimulatedHandler(0.0))
	registry.Register("transform", job.SimulatedHandler(0.3)) // fails 30% of the time, exercises retry
	registry.Register("load", job.SimulatedHandler(0.0))

	// seed a small DAG: extract -> transform -> load
	seed := []*job.Job{
		job.NewJob("extract-1", "extract", map[string]string{"src": "s3://raw"}, nil, 3),
		job.NewJob("transform-1", "transform", map[string]string{"step": "clean"}, []string{"extract-1"}, 3),
		job.NewJob("load-1", "load", map[string]string{"dst": "warehouse"}, []string{"transform-1"}, 3),
	}
	for _, j := range seed {
		if s.Get(j.ID) == nil { // don't re-seed on restart
			if err := s.Put(j); err != nil {
				log.Fatalf("failed to seed job %s: %v", j.ID, err)
			}
		}
	}

	sch := scheduler.NewScheduler(s, 100, 200*time.Millisecond)
	pool := scheduler.NewWorkerPool(s, registry, sch, 4)
	
	done := make(chan struct{})
	go sch.Run(done)
	pool.Start()

	// let the pipeline run for a few seconds, then shut down
	time.Sleep(5 * time.Second)
	close(done)
	pool.Wait()

	log.Println("--- final job states ---")
	for _, j := range s.All() {
		log.Printf("%-12s type=%-10s status=%-10s attempts=%d err=%q",
			j.ID, j.Type, j.Status, j.Attempts, j.Error)
	}
}