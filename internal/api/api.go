package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/Harsha85018/dispatchd/internal/job"
	"github.com/Harsha85018/dispatchd/internal/store"
)

type Server struct {
	store *store.Store
}

func NewServer(s *store.Store) *Server {
	return &Server{store: s}
}

// SubmitRequest is the body accepted by POST /jobs.
type SubmitRequest struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Payload     map[string]string `json:"payload"`
	DependsOn   []string          `json:"depends_on"`
	MaxAttempts int               `json:"max_attempts"`
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/jobs", s.handleJobs)   // POST to submit, GET to list
	mux.HandleFunc("/jobs/", s.handleJobByID) // GET /jobs/{id}
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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