package store

import (
	"os"
	"testing"
	"time"

	"github.com/Harsha85018/dispatchd/internal/job"
)

// newTestStore creates a store backed by a temp WAL that is cleaned up
// when the test finishes.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	path := t.TempDir() + "/test.wal"
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		os.Remove(path)
	})
	return s
}

func TestLeaseReturnsPendingJob(t *testing.T) {
	s := newTestStore(t)

	if err := s.Put(job.NewJob("j1", "test", nil, nil, 3)); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	leased, err := s.Lease("worker-1", 10*time.Second)
	if err != nil {
		t.Fatalf("lease failed: %v", err)
	}
	if leased == nil {
		t.Fatal("expected a job, got nil")
	}
	if leased.Status != job.StatusLeased {
		t.Errorf("expected status leased, got %s", leased.Status)
	}
	if leased.LeasedBy != "worker-1" {
		t.Errorf("expected leased_by worker-1, got %s", leased.LeasedBy)
	}
	if leased.LeaseToken == "" {
		t.Error("expected a lease token, got empty string")
	}
	if leased.Attempts != 1 {
		t.Errorf("expected attempts 1, got %d", leased.Attempts)
	}
}

// A job already leased must not be handed to a second worker.
func TestLeaseIsExclusive(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))

	first, _ := s.Lease("worker-1", 10*time.Second)
	if first == nil {
		t.Fatal("first lease returned nil")
	}

	second, err := s.Lease("worker-2", 10*time.Second)
	if err != nil {
		t.Fatalf("second lease errored: %v", err)
	}
	if second != nil {
		t.Fatalf("expected nil for second worker, got job %s", second.ID)
	}
}

func TestLeaseRespectsDependencies(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("parent", "test", nil, nil, 3))
	s.Put(job.NewJob("child", "test", nil, []string{"parent"}, 3))

	// only the parent should be leasable while it is unfinished
	first, _ := s.Lease("worker-1", 10*time.Second)
	if first == nil || first.ID != "parent" {
		t.Fatalf("expected parent, got %v", first)
	}

	if next, _ := s.Lease("worker-2", 10*time.Second); next != nil {
		t.Fatalf("child should not be leasable yet, got %s", next.ID)
	}

	// once the parent succeeds, the child becomes available
	if err := s.Complete("parent", "worker-1", first.LeaseToken, true, ""); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	child, _ := s.Lease("worker-2", 10*time.Second)
	if child == nil || child.ID != "child" {
		t.Fatalf("expected child to be leasable, got %v", child)
	}
}

// This is the bug that motivated lease tokens: the same worker re-leases a
// job it previously lost, and the old attempt then reports its result.
// Worker ID alone would accept that stale write.
func TestStaleTokenIsRejected(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))

	// worker-1 leases, then loses the lease to the reaper
	first, _ := s.Lease("worker-1", 1*time.Millisecond)
	staleToken := first.LeaseToken

	time.Sleep(5 * time.Millisecond)
	if _, err := s.ReapExpired(); err != nil {
		t.Fatalf("reap failed: %v", err)
	}

	// the SAME worker leases it again, getting a fresh token
	second, _ := s.Lease("worker-1", 10*time.Second)
	if second == nil {
		t.Fatal("expected job to be leasable after reap")
	}
	if second.LeaseToken == staleToken {
		t.Fatal("expected a new token on re-lease")
	}

	// the abandoned first attempt now finishes and tries to report
	err := s.Complete("j1", "worker-1", staleToken, true, "")
	if err != ErrLeaseLost {
		t.Fatalf("expected ErrLeaseLost for stale token, got %v", err)
	}

	// and the job must still be leased to the live attempt, not succeeded
	current := s.Get("j1")
	if current.Status != job.StatusLeased {
		t.Errorf("expected job to remain leased, got %s", current.Status)
	}
}

func TestRenewExtendsLease(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))

	leased, _ := s.Lease("worker-1", 50*time.Millisecond)
	firstExpiry := *leased.LeaseExpiry

	time.Sleep(10 * time.Millisecond)
	if err := s.Renew("j1", "worker-1", leased.LeaseToken, 50*time.Millisecond); err != nil {
		t.Fatalf("renew failed: %v", err)
	}

	renewed := s.Get("j1")
	if !renewed.LeaseExpiry.After(firstExpiry) {
		t.Error("expected lease expiry to move forward after renew")
	}
}

func TestRenewRejectsWrongToken(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))
	s.Lease("worker-1", 10*time.Second)

	if err := s.Renew("j1", "worker-1", "not-the-real-token", 10*time.Second); err != ErrLeaseLost {
		t.Fatalf("expected ErrLeaseLost, got %v", err)
	}
}

// An expired lease must return the job to the queue so another worker
// can pick it up — this is what makes a dead worker recoverable.
func TestReapExpiredRequeuesJob(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))

	s.Lease("worker-1", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	reaped, err := s.ReapExpired()
	if err != nil {
		t.Fatalf("reap failed: %v", err)
	}
	if len(reaped) != 1 || reaped[0] != "j1" {
		t.Fatalf("expected j1 to be reaped, got %v", reaped)
	}

	after := s.Get("j1")
	if after.Status != job.StatusPending {
		t.Errorf("expected pending after reap, got %s", after.Status)
	}
	if after.LeasedBy != "" || after.LeaseToken != "" {
		t.Error("expected lease fields to be cleared after reap")
	}
}

// A healthy lease must be left alone by the reaper.
func TestReapLeavesLiveLeaseAlone(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 3))
	s.Lease("worker-1", 10*time.Second)

	reaped, _ := s.ReapExpired()
	if len(reaped) != 0 {
		t.Fatalf("expected no jobs reaped, got %v", reaped)
	}
}

func TestFailedJobRequeuesUntilMaxAttempts(t *testing.T) {
	s := newTestStore(t)
	s.Put(job.NewJob("j1", "test", nil, nil, 2))

	// attempt 1 fails -> back to pending
	first, _ := s.Lease("worker-1", 10*time.Second)
	s.Complete("j1", "worker-1", first.LeaseToken, false, "boom")
	if got := s.Get("j1").Status; got != job.StatusPending {
		t.Fatalf("expected pending after first failure, got %s", got)
	}

	// attempt 2 fails -> exhausted, marked failed
	second, _ := s.Lease("worker-1", 10*time.Second)
	s.Complete("j1", "worker-1", second.LeaseToken, false, "boom again")
	if got := s.Get("j1").Status; got != job.StatusFailed {
		t.Fatalf("expected failed after max attempts, got %s", got)
	}
}
