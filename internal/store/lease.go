package store

import (
	"time"
	"crypto/rand"
	"encoding/hex"

	"github.com/Harsha85018/dispatchd/internal/job"
)

// Lease atomically finds one pending job whose dependencies are satisfied,
// marks it leased to workerID for the given duration, and returns a copy.
// Returns nil if no job is currently available.
//
// Candidates come from the pending index rather than a scan of every job,
// so lease cost tracks the number of pending jobs, not the size of the store.
func (s *Store) Lease(workerID string, duration time.Duration) (*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.pending {
		j, ok := s.jobs[id]
		if !ok || j.Status != job.StatusPending {
			// index disagrees with the job map; drop the stale entry
			delete(s.pending, id)
			continue
		}
		if !s.dependenciesMet(j) {
			continue
		}

		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}

		expiry := time.Now().UTC().Add(duration)

		j.Status = job.StatusLeased
		j.LeasedBy = workerID
		j.LeaseToken = token
		j.LeaseExpiry = &expiry
		j.Attempts++
		j.UpdatedAt = time.Now().UTC()

		if err := s.appendWAL(j); err != nil {
			return nil, err
		}
		s.syncPending(j)

		cp := *j
		return &cp, nil
	}

	return nil, nil
}

// Complete records the terminal (or retry) outcome of a leased job.
// It verifies the caller still holds the lease before applying the change,
// so a worker whose lease already expired cannot clobber a reassigned job.
func (s *Store) Complete(id, workerID, token string, success bool, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if j.LeasedBy != workerID || j.LeaseToken != token {
		return ErrLeaseLost
	}

	j.LeasedBy = ""
	j.LeaseToken = ""
	j.LeaseExpiry = nil
	j.UpdatedAt = time.Now().UTC()

	switch {
	case success:
		j.Status = job.StatusSucceeded
		j.Error = ""
	case j.Attempts < j.MaxAttempts:
		j.Status = job.StatusPending // requeue for another attempt
		j.Error = errMsg
	default:
		j.Status = job.StatusFailed
		j.Error = errMsg
	}

	s.syncPending(j)
	return s.appendWAL(j)
}

// ReapExpired requeues any leased job whose lease has expired, which is how
// the system recovers work from workers that died mid-job. Returns the IDs
// that were reaped.
func (s *Store) ReapExpired() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var reaped []string

	for _, j := range s.jobs {
		if j.Status != job.StatusLeased || j.LeaseExpiry == nil || j.LeaseExpiry.After(now) {
			continue
		}

		j.LeasedBy = ""
		j.LeaseToken = ""
		j.LeaseExpiry = nil
		j.UpdatedAt = now

		if j.Attempts < j.MaxAttempts {
			j.Status = job.StatusPending
			j.Error = "lease expired, requeued"
		} else {
			j.Status = job.StatusFailed
			j.Error = "lease expired after max attempts"
		}

		if err := s.appendWAL(j); err != nil {
			return reaped, err
		}
		s.syncPending(j)
		reaped = append(reaped, j.ID)
	}

	return reaped, nil
}

// dependenciesMet reports whether every job this one depends on has
// succeeded. A dependency that doesn't exist counts as unmet, so a job
// never runs before its prerequisites are actually present.
// The caller must hold s.mu.
func (s *Store) dependenciesMet(j *job.Job) bool {
	for _, depID := range j.DependsOn {
		dep, ok := s.jobs[depID]
		if !ok || dep.Status != job.StatusSucceeded {
			return false
		}
	}
	return true
}

// Renew extends the lease on a job the worker still holds. This is how a
// long-running job avoids being reaped: the worker heartbeats while it works,
// and only stops if it dies. Returns ErrLeaseLost if the worker no longer
// holds the lease, which tells it to abandon the work.
func (s *Store) Renew(id, workerID, token string, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if j.Status != job.StatusLeased || j.LeasedBy != workerID || j.LeaseToken != token {
		return ErrLeaseLost
	}

	expiry := time.Now().UTC().Add(duration)
	j.LeaseExpiry = &expiry
	j.UpdatedAt = time.Now().UTC()

	return s.appendWAL(j)
}

// newLeaseToken returns a random token identifying one specific lease.
// Worker ID alone is not enough: the same worker can re-lease a job it
// previously lost, and a stale result from the earlier attempt would
// otherwise pass validation.
func newLeaseToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}