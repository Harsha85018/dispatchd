package job

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Handler executes a job of a particular type. The context is cancelled if
// the worker loses its lease, so handlers should abandon work when it fires
// rather than finishing something nobody will accept.
type Handler func(ctx context.Context, j *Job) error

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
	return func(ctx context.Context, j *Job) error {
		delay := time.Duration(50+rand.Intn(150)) * time.Millisecond

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		if rand.Float64() < failureRate {
			return fmt.Errorf("simulated failure executing job %s", j.ID)
		}
		return nil
	}
}
