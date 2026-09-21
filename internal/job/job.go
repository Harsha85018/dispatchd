package job

import "time"

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusLeased    Status = "leased"
)

// Job represents a single unit of work to be scheduled and executed.
type Job struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`         // identifies which handler should run this job
	Payload     map[string]string `json:"payload"`       // arbitrary key-value input for the job
	Status      Status            `json:"status"`
	DependsOn   []string          `json:"depends_on"`    // job IDs that must succeed before this one runs
	Attempts    int               `json:"attempts"`
	LeasedBy    string    `json:"leased_by,omitempty"`
	LeaseExpiry *time.Time `json:"lease_expiry,omitempty"`
	MaxAttempts int               `json:"max_attempts"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Error       string            `json:"error,omitempty"`
}

// NewJob creates a new job in the pending state.
func NewJob(id, jobType string, payload map[string]string, dependsOn []string, maxAttempts int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:          id,
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		DependsOn:   dependsOn,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}