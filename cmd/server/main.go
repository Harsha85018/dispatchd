package main

import (
	"log"
	"net/http"
	"time"

	"github.com/Harsha85018/dispatchd/internal/api"
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

	registry := job.NewRegistry()
	registry.Register("extract", job.SimulatedHandler(0.0))
	registry.Register("transform", job.SimulatedHandler(0.3))
	registry.Register("load", job.SimulatedHandler(0.0))

	sch := scheduler.NewScheduler(s, 100, 200*time.Millisecond)
	pool := scheduler.NewWorkerPool(s, registry, sch, 4)

	done := make(chan struct{})
	go sch.Run(done)
	pool.Start()

	srv := api.NewServer(s)
	log.Println("dispatchd listening on :8080")
	if err := http.ListenAndServe(":8080", srv.Routes()); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}