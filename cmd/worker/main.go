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

	"github.com/Harsha85018/dispatchd/internal/job"
)

var (
	serverURL = flag.String("server", "http://localhost:8080", "dispatchd server URL")
	workerID  = flag.String("id", "", "unique worker id (defaults to hostname+pid)")
	pollWait  = flag.Duration("poll", 500*time.Millisecond, "how long to wait when no work is available")
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
			report(client, id, j.ID, false, "no handler registered for job type")
			continue
		}

		runErr := handler(j)
		if runErr != nil {
			log.Printf("worker %s: job %s failed: %v", id, j.ID, runErr)
			report(client, id, j.ID, false, runErr.Error())
			continue
		}

		log.Printf("worker %s: job %s succeeded", id, j.ID)
		report(client, id, j.ID, true, "")
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
func report(client *http.Client, workerID, jobID string, success bool, errMsg string) {
	payload := map[string]interface{}{
		"job_id":    jobID,
		"worker_id": workerID,
		"success":   success,
		"error":     errMsg,
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