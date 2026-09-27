package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
	"context"

	"github.com/Harsha85018/dispatchd/internal/job"
)

var (
	serverURL = flag.String("server", "http://localhost:8080", "dispatchd server URL")
	workerID  = flag.String("id", "", "unique worker id (defaults to hostname+pid)")
	pollWait  = flag.Duration("poll", 500*time.Millisecond, "how long to wait when no work is available")
	heartbeatInterval = flag.Duration("heartbeat", 3*time.Second, "how often to renew the lease while running a job")
)

func main() {
	flag.Parse()

	id := *workerID
	if id == "" {
		host, _ := os.Hostname()
		id = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	registry := job.NewRegistry()
	registry.Register("extract", job.SimulatedHandler(0.0))
	registry.Register("transform", job.SimulatedHandler(0.3))
	registry.Register("load", job.SimulatedHandler(0.0))
	registry.Register("slow", slowHandler(30*time.Second))
	registry.Register("noop", func(ctx context.Context, j *job.Job) error { return nil })

	client := &http.Client{Timeout: 5 * time.Second}
	log.Printf("worker %s started, polling %s", id, *serverURL)

	for {
		j, err := lease(client, id)
		if err != nil {
			log.Printf("worker %s: lease request failed: %v", id, err)
			time.Sleep(*pollWait)
			continue
		}
		if j == nil {
			time.Sleep(*pollWait)
			continue
		}

		log.Printf("worker %s: leased job %s (type=%s, attempt %d/%d)",
			id, j.ID, j.Type, j.Attempts, j.MaxAttempts)

		handler, ok := registry.Get(j.Type)
		if !ok {
			report(client, id, j.ID, j.LeaseToken, false, "no handler registered for job type")
			continue
		}

		// heartbeat the lease while the handler runs. If the lease is lost,
		// the heartbeat cancels ctx so the handler stops work it can no
		// longer report.
		ctx, cancel := context.WithCancel(context.Background())
		stop := make(chan struct{})
		lost := make(chan struct{})
		go heartbeat(client, id, j.ID, j.LeaseToken, *heartbeatInterval, stop, cancel, lost)

		runErr := handler(ctx, j)
		close(stop)
		cancel() // always release the context, success or not

		// if the lease was lost mid-execution, the job has already been
		// reassigned — reporting now would be a stale write
		select {
		case <-lost:
			log.Printf("worker %s: discarding result for %s (lease lost)", id, j.ID)
			continue
		default:
		}

		if runErr != nil {
			log.Printf("worker %s: job %s failed: %v", id, j.ID, runErr)
			report(client, id, j.ID, j.LeaseToken, false, runErr.Error())
			continue
		}

		log.Printf("worker %s: job %s succeeded", id, j.ID)
		report(client, id, j.ID, j.LeaseToken, true, "")
	}
}

// lease asks the server for a job. A nil job with nil error means
// nothing is available right now.
func lease(client *http.Client, id string) (*job.Job, error) {
	body, err := json.Marshal(map[string]string{"worker_id": id})
	if err != nil {
		return nil, err
	}

	resp, err := client.Post(*serverURL+"/lease", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var j job.Job
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		return nil, err
	}
	return &j, nil
}

// report tells the server the outcome of a leased job.
func report(client *http.Client, workerID, jobID, token string, success bool, errMsg string) {
	payload := map[string]interface{}{
		"job_id":      jobID,
		"worker_id":   workerID,
		"lease_token": token,
		"success":     success,
		"error":       errMsg,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("worker %s: failed to encode completion for %s: %v", workerID, jobID, err)
		return
	}

	resp, err := client.Post(*serverURL+"/complete", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("worker %s: failed to report completion for %s: %v", workerID, jobID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		// our lease expired and the job was reassigned; the result is stale
		log.Printf("worker %s: lease lost for job %s, result discarded", workerID, jobID)
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("worker %s: unexpected status %d reporting job %s", workerID, resp.StatusCode, jobID)
	}
}


// renew extends this worker's lease on a job. It returns false if the
// lease is gone, which means the job was reaped and reassigned while we
// were still working on it.
func renew(client *http.Client, workerID, jobID, token string) bool {
	body, err := json.Marshal(map[string]string{
		"job_id":      jobID,
		"worker_id":   workerID,
		"lease_token": token,
	})
	if err != nil {
		return false
	}

	resp, err := client.Post(*serverURL+"/renew", "application/json", bytes.NewReader(body))
	if err != nil {
		// a transient network error shouldn't abandon the job; the next
		// heartbeat may well succeed before the lease actually expires
		log.Printf("worker %s: renew request failed for %s: %v", workerID, jobID, err)
		return true
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("worker %s: renewed lease on %s", workerID, jobID)
	}
	return resp.StatusCode == http.StatusOK
}


// heartbeat renews the lease every interval until stop is closed.
// If a renewal shows the lease is lost, it closes lost so the caller
// can discard the result.
func heartbeat(client *http.Client, workerID, jobID, token string, interval time.Duration, stop <-chan struct{}, cancel context.CancelFunc, lost chan<- struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !renew(client, workerID, jobID, token) {
				log.Printf("worker %s: lost lease on %s, cancelling work", workerID, jobID)
				close(lost)
				cancel()
				return
			}
		}
	}
}



// slowHandler simulates long-running work, so a worker can be killed
// mid-job to test lease expiry and reassignment.
func slowHandler(d time.Duration) job.Handler {
	return func(ctx context.Context, j *job.Job) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}
}