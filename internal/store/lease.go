package store

import (
	"time"

	"github.com/Harsha85018/dispatchd/internal/job"
)

// Lease atomically finds one pending job whose dependencies are satisfied,
// marks it leased to workerID for the given duration, and returns a copy.
// Returns nil if no job is currently available.
//
// The whole operation happens under the write lock, so two workers polling
// concurrently can never be handed the same job.
func (s *Store) Lease(workerID string, duration time.Duration) (*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// snapshot statuses for dependency checks
	statusByID := make(map[string]job.Status, len(s.jobs))
	for id, j := range s.jobs {
		statusByID[id] = j.Status
	}

	for _, j := range s.jobs {
		if j.Status != job.StatusPending {
			continue
		}
		if !dependenciesSatisfied(j, statusByID) {
			continue
		}

		expiry := time.Now().UTC().Add(duration)

		j.Status = job.StatusLeased
		j.LeasedBy = workerID
		j.LeaseExpiry = &expiry
		j.Attempts++
		j.UpdatedAt = time.Now().UTC()

		if err := s.appendWAL(j); err != nil {
			return nil, err
		}

		cp := *j
		return &cp, nil
	}

	return nil, nil
}

// Complete records the terminal (or retry) outcome of a leased job.
// It verifies the caller still holds the lease before applying the change,
// so a worker whose lease already expired cannot clobber a reassigned job.
func (s *Store) Complete(id, workerID string, success bool, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if j.LeasedBy != workerID {
		return ErrLeaseLost
	}

	j.LeasedBy = ""
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
		reaped = append(reaped, j.ID)
	}

	return reaped, nil
}

func dependenciesSatisfied(j *job.Job, statusByID map[string]job.Status) bool {
	for _, depID := range j.DependsOn {
		status, ok := statusByID[depID]
		if !ok || status != job.StatusSucceeded {
			return false
		}
	}
	return true
}