package main

import (
	"context"
	"testing"
	"time"

	"github.com/Harsha85018/dispatchd/internal/job"
)

// A handler must abandon its work when the context is cancelled, rather
// than running to completion on a job the worker no longer owns.
func TestSlowHandlerStopsOnCancel(t *testing.T) {
	handler := slowHandler(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	j := job.NewJob("j1", "slow", nil, nil, 3)

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- handler(ctx, j) }()

	// simulate the heartbeat discovering the lease is gone
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("handler took %v to stop; it should abandon work promptly", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not stop after cancellation — it ran on regardless")
	}
}

// Without cancellation the same handler must still complete normally.
func TestSlowHandlerCompletesWhenNotCancelled(t *testing.T) {
	handler := slowHandler(20 * time.Millisecond)

	if err := handler(context.Background(), job.NewJob("j1", "slow", nil, nil, 3)); err != nil {
		t.Errorf("expected clean completion, got %v", err)
	}
}
