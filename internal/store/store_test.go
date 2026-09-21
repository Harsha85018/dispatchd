package store

import (
	"os"
	"testing"

	"github.com/Harsha85018/dispatchd/internal/job"
)

func TestPutAndGet(t *testing.T) {
	path := "test_wal.log"
	defer os.Remove(path)

	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	j := job.NewJob("job-1", "send_email", map[string]string{"to": "a@b.com"}, nil, 3)
	if err := s.Put(j); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}

	got := s.Get("job-1")
	if got == nil {
		t.Fatal("expected job-1 to exist, got nil")
	}
	if got.Type != "send_email" {
		t.Errorf("expected type send_email, got %s", got.Type)
	}
}

func TestReplayAfterRestart(t *testing.T) {
	path := "test_wal_replay.log"
	defer os.Remove(path)

	// simulate a first "process": write a job, then close (like a crash)
	s1, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	j := job.NewJob("job-2", "resize_image", map[string]string{"file": "cat.png"}, nil, 3)
	if err := s1.Put(j); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}
	s1.Close()

	// simulate restart: open a NEW store pointed at the same WAL file
	s2, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer s2.Close()

	got := s2.Get("job-2")
	if got == nil {
		t.Fatal("expected job-2 to survive restart via WAL replay, got nil")
	}
	if got.Type != "resize_image" {
		t.Errorf("expected type resize_image, got %s", got.Type)
	}
}