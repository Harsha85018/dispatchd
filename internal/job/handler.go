package job

import (
	"fmt"
	"math/rand"
	"time"
)

// Handler executes a job of a particular type.
// Returning an error marks the attempt as failed.
type Handler func(j *Job) error

// Registry maps job types to their handlers.
type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

// SimulatedHandler is a stand-in handler for development: it sleeps for a
// short random duration and fails with the given probability, so we can
// exercise retry and failure paths before real work is plugged in.
func SimulatedHandler(failureRate float64) Handler {
	return func(j *Job) error {
		time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
		if rand.Float64() < failureRate {
			return fmt.Errorf("simulated failure executing job %s", j.ID)
		}
		return nil
	}
}