package main

import (
	"log"
	"net/http"
	"time"

	"github.com/Harsha85018/dispatchd/internal/api"
	"github.com/Harsha85018/dispatchd/internal/scheduler"
	"github.com/Harsha85018/dispatchd/internal/store"
)

const (
	leaseDuration  = 10 * time.Second
	reaperInterval = 2 * time.Second
)

func main() {
	s, err := store.NewStore("dispatchd.wal")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	done := make(chan struct{})
	go scheduler.RunReaper(s, reaperInterval, done)

	srv := api.NewServer(s, leaseDuration)
	log.Println("dispatchd server listening on :8080")
	if err := http.ListenAndServe(":8080", srv.Routes()); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}