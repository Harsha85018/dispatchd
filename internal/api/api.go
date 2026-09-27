package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
	"errors"
	"os"

	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/store"
)

// JobStore is the subset of store behavior the API needs. Both the
// file-backed and Postgres stores satisfy it.
type JobStore interface {
	Put(j *job.Job) error
	Get(id string) *job.Job
	All() []*job.Job
	Lease(workerID string, duration time.Duration) (*job.Job, error)
	Renew(id, workerID, token string, duration time.Duration) error
	Complete(id, workerID, token string, success bool, errMsg string) error
}

type Server struct {
	store         JobStore
	leaseDuration time.Duration
	hostname      string
}

func NewServer(s JobStore, leaseDuration time.Duration) *Server {
	host, _ := os.Hostname()
	return &Server{store: s, leaseDuration: leaseDuration, hostname: host}
}


// SubmitRequest is the body accepted by POST /jobs.
type SubmitRequest struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Payload     map[string]string `json:"payload"`
	DependsOn   []string          `json:"depends_on"`
	MaxAttempts int               `json:"max_attempts"`
}

// LeaseRequest is the body accepted by POST /lease.
type LeaseRequest struct {
	WorkerID string `json:"worker_id"`
}

// CompleteRequest is the body accepted by POST /complete.
type CompleteRequest struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	LeaseToken string `json:"lease_token"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
}

// RenewRequest is the body accepted by POST /renew.
type RenewRequest struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	LeaseToken string `json:"lease_token"`
}


func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/jobs", s.handleJobs)   // POST to submit, GET to list
	mux.HandleFunc("/jobs/", s.handleJobByID) // GET /jobs/{id}
	mux.HandleFunc("/lease", s.handleLease)
	mux.HandleFunc("/renew", s.handleRenew)
	mux.HandleFunc("/complete", s.handleComplete)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"server": s.hostname,
	})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.submitJob(w, r)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.store.All())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.ID == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "id and type are required")
		return
	}
	if s.store.Get(req.ID) != nil {
		writeError(w, http.StatusConflict, "job with this id already exists")
		return
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}

	j := job.NewJob(req.ID, req.Type, req.Payload, req.DependsOn, req.MaxAttempts)
	if err := s.store.Put(j); err != nil {
		log.Printf("api: failed to persist job %s: %v", j.ID, err)
		writeError(w, http.StatusInternalServerError, "failed to persist job")
		return
	}

	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job id required")
		return
	}

	j := s.store.Get(id)
	if j == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// handleLease hands out at most one ready job to a worker, with a lease.
// A 204 means nothing is available right now — workers poll again shortly.
func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req LeaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}

	j, err := s.store.Lease(req.WorkerID, s.leaseDuration)
	if err != nil {
		log.Printf("api: lease failed for worker %s: %v", req.WorkerID, err)
		writeError(w, http.StatusInternalServerError, "failed to lease job")
		return
	}
	if j == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	writeJSON(w, http.StatusOK, j)
}

// handleComplete records the outcome of a leased job.
func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req CompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.JobID == "" || req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "job_id and worker_id are required")
		return
	}

	err := s.store.Complete(req.JobID, req.WorkerID, req.LeaseToken, req.Success, req.Error)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrLeaseLost):
		// the lease expired and the job was reassigned — the worker's
		// result is stale and must be discarded
		writeError(w, http.StatusConflict, "lease no longer held")
	case err != nil:
		log.Printf("api: complete failed for job %s: %v", req.JobID, err)
		writeError(w, http.StatusInternalServerError, "failed to complete job")
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// handleRenew extends a worker's lease on a job it is still running.
// A 409 tells the worker its lease was already reaped and the job
// reassigned, so it should stop working and discard its result.
func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req RenewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.JobID == "" || req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "job_id and worker_id are required")
		return
	}

	err := s.store.Renew(req.JobID, req.WorkerID, req.LeaseToken, s.leaseDuration)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrLeaseLost):
		writeError(w, http.StatusConflict, "lease no longer held")
	case err != nil:
		log.Printf("api: renew failed for job %s: %v", req.JobID, err)
		writeError(w, http.StatusInternalServerError, "failed to renew lease")
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}


func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: failed to encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}