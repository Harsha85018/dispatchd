package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Harsha85018/dispatchd/internal/api"
	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/scheduler"
	"github.com/Harsha85018/dispatchd/internal/store"
)

// jobStore is what the API and reaper need from a store, so the
// file-backed and Postgres implementations are interchangeable.
type jobStore interface {
	Put(j *job.Job) error
	Get(id string) *job.Job
	All() []*job.Job
	Lease(workerID string, duration time.Duration) (*job.Job, error)
	Renew(id, workerID, token string, duration time.Duration) error
	Complete(id, workerID, token string, success bool, errMsg string) error
	ReapExpired() ([]string, error)
	Close() error
}

var (
	leaseDuration  = flag.Duration("lease", 10*time.Second, "how long a lease is valid")
	reaperInterval = flag.Duration("reaper", 2*time.Second, "how often to reap expired leases")
	walPath        = flag.String("wal", "dispatchd.wal", "path to the write-ahead log")
	dbDSN          = flag.String("db", "", "Postgres DSN; if empty, uses the file-backed WAL store")
)

func main() {
	flag.Parse()

	var s jobStore
	if *dbDSN != "" {
		ps, err := store.NewPostgresStore(context.Background(), *dbDSN)
		if err != nil {
			log.Fatalf("failed to connect to postgres: %v", err)
		}
		log.Println("using postgres store")
		s = ps
	} else {
		fs, err := store.NewStore(*walPath)
		if err != nil {
			log.Fatalf("failed to open store: %v", err)
		}
		log.Println("using file-backed WAL store")
		s = fs
	}
	defer s.Close()

	done := make(chan struct{})
	go scheduler.RunReaper(s, *reaperInterval, done)

	srv := api.NewServer(s, *leaseDuration)
	log.Println("dispatchd server listening on :8080")
	if err := http.ListenAndServe(":8080", srv.Routes()); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}