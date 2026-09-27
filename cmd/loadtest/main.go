package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

var (
	serverURL   = flag.String("server", "http://localhost:8080", "dispatchd server URL")
	numJobs     = flag.Int("jobs", 1000, "number of jobs to submit")
	concurrency = flag.Int("concurrency", 20, "parallel submission requests")
	jobType     = flag.String("type", "noop", "job type to submit")
	timeout     = flag.Duration("timeout", 2*time.Minute, "give up waiting after this long")
)

type jobState struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Attempts  int       `json:"attempts"`
}

func main() {
	flag.Parse()

	prefix := fmt.Sprintf("load-%d", time.Now().Unix())
	log.Printf("submitting %d jobs (type=%s, concurrency=%d)", *numJobs, *jobType, *concurrency)

	submitStart := time.Now()
	submitJobs(prefix)
	submitElapsed := time.Since(submitStart)

	log.Printf("submitted in %v (%.0f jobs/sec)", submitElapsed.Round(time.Millisecond),
		float64(*numJobs)/submitElapsed.Seconds())

	log.Println("waiting for completion...")
	jobs, drainElapsed, err := waitForCompletion(prefix)
	if err != nil {
		log.Fatalf("load test failed: %v", err)
	}

	report(jobs, submitElapsed, drainElapsed)
}

func submitJobs(prefix string) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, *concurrency)
	client := &http.Client{Timeout: 10 * time.Second}

	for i := 0; i < *numJobs; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(n int) {
			defer wg.Done()
			defer func() { <-sem }()

			body, _ := json.Marshal(map[string]interface{}{
				"id":   fmt.Sprintf("%s-%d", prefix, n),
				"type": *jobType,
			})

			resp, err := client.Post(*serverURL+"/jobs", "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("submit %d failed: %v", n, err)
				return
			}
			resp.Body.Close()
		}(i)
	}

	wg.Wait()
}

// waitForCompletion polls until every job in this run reaches a terminal
// state, and returns how long the queue took to drain after submission.
func waitForCompletion(prefix string) ([]jobState, time.Duration, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	deadline := time.Now().Add(*timeout)
	start := time.Now()

	for time.Now().Before(deadline) {
		resp, err := client.Get(*serverURL + "/jobs")
		if err != nil {
			return nil, 0, err
		}

		var all []jobState
		if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
			resp.Body.Close()
			return nil, 0, err
		}
		resp.Body.Close()

		var mine []jobState
		pending := 0
		for _, j := range all {
			if len(j.ID) < len(prefix) || j.ID[:len(prefix)] != prefix {
				continue
			}
			mine = append(mine, j)
			if j.Status != "succeeded" && j.Status != "failed" {
				pending++
			}
		}

		if len(mine) == *numJobs && pending == 0 {
			return mine, time.Since(start), nil
		}

		time.Sleep(250 * time.Millisecond)
	}

	return nil, 0, fmt.Errorf("timed out after %v", *timeout)
}

func report(jobs []jobState, submitElapsed, drainElapsed time.Duration) {
	// end-to-end latency: submission to terminal state, per job
	latencies := make([]time.Duration, 0, len(jobs))
	succeeded, failed, totalAttempts := 0, 0, 0

	for _, j := range jobs {
		latencies = append(latencies, j.UpdatedAt.Sub(j.CreatedAt))
		totalAttempts += j.Attempts
		if j.Status == "succeeded" {
			succeeded++
		} else {
			failed++
		}
	}

	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })

	fmt.Println()
	fmt.Println("=== dispatchd load test ===")
	fmt.Printf("jobs:            %d (%d succeeded, %d failed)\n", len(jobs), succeeded, failed)
	fmt.Printf("total attempts:  %d\n", totalAttempts)
	fmt.Printf("submit time:     %v (%.0f jobs/sec)\n",
		submitElapsed.Round(time.Millisecond), float64(len(jobs))/submitElapsed.Seconds())
	fmt.Printf("drain time:      %v (%.0f jobs/sec)\n",
		drainElapsed.Round(time.Millisecond), float64(len(jobs))/drainElapsed.Seconds())
	fmt.Println()
	fmt.Println("end-to-end latency (submit -> terminal state):")
	fmt.Printf("  p50:  %v\n", percentile(latencies, 50).Round(time.Millisecond))
	fmt.Printf("  p95:  %v\n", percentile(latencies, 95).Round(time.Millisecond))
	fmt.Printf("  p99:  %v\n", percentile(latencies, 99).Round(time.Millisecond))
	fmt.Printf("  max:  %v\n", latencies[len(latencies)-1].Round(time.Millisecond))
	fmt.Println()
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}