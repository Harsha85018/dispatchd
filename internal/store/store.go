package store

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"errors"

	"github.com/Harsha85018/dispatchd/internal/job"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("lease no longer held by this worker")
)
// Store is a durable, WAL-backed store for jobs.
// Every mutation is appended to a log file before being applied in memory,
// so state can be rebuilt by replaying the log after a crash.
type Store struct {
	mu       sync.RWMutex
	jobs     map[string]*job.Job
	walFile  *os.File
	walPath  string
	pending  map[string]bool // ids of jobs in pending state, for O(1) lease lookup
}

// walEntry is a single record appended to the write-ahead log.
type walEntry struct {
	Op  string   `json:"op"` // "put"
	Job *job.Job `json:"job"`
}

// NewStore opens (or creates) the WAL file at walPath and replays it
// to rebuild in-memory state.
func NewStore(walPath string) (*Store, error) {
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	s := &Store{
		jobs:    make(map[string]*job.Job),
		pending: make(map[string]bool),
		walFile: f,
		walPath: walPath,
	}

	if err := s.replay(); err != nil {
		return nil, err
	}

	return s, nil
}

// replay reads every entry in the WAL from the start and rebuilds
// the in-memory map. Called once at startup.
func (s *Store) replay() error {
	f, err := os.Open(s.walPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// allow larger lines than the default 64KB buffer, in case payloads grow
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		var entry walEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return err
		}
		if entry.Op == "put" {
			s.jobs[entry.Job.ID] = entry.Job
			s.syncPending(entry.Job)
		}
	}
	return scanner.Err()
}

// Put writes the job to the WAL, then applies it in memory.
// The WAL write happens first and is fsynced, so a crash between
// the two steps can never lose the write.
func (s *Store) Put(j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.appendWAL(j); err != nil {
		return err
	}

	cp := *j
	s.jobs[j.ID] = &cp
	s.syncPending(&cp)
	return nil
}


// appendWAL writes the job to the log and fsyncs. The caller must already
// hold s.mu. The in-memory map is assumed to already reference this job.
func (s *Store) appendWAL(j *job.Job) error {
	entry := walEntry{Op: "put", Job: j}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	if _, err := s.walFile.Write(line); err != nil {
		return err
	}
	return s.walFile.Sync()
}

// syncPending keeps the pending index in step with a job's status.
// The caller must hold s.mu.
func (s *Store) syncPending(j *job.Job) {
	if j.Status == job.StatusPending {
		s.pending[j.ID] = true
	} else {
		delete(s.pending, j.ID)
	}
}

// Get returns a copy of the job with the given ID, or nil if it doesn't exist.
// A copy is returned so callers cannot mutate shared state outside the lock.
func (s *Store) Get(id string) *job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil
	}
	cp := *j
	return &cp
}

// All returns copies of all jobs currently in the store.
func (s *Store) All() []*job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		cp := *j
		out = append(out, &cp)
	}
	return out
}

// Close closes the underlying WAL file.
func (s *Store) Close() error {
	return s.walFile.Close()
}