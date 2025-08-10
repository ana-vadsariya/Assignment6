package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

type Task struct {
	ID      string
	Payload string
}

func main() {
	numWorkers := 4
	numTasks := 20
	outFile := "results_go.txt"

	log.SetPrefix("[ride-go] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// channels
	taskCh := make(chan Task)
	resultCh := make(chan string)
	var wg sync.WaitGroup

	// writer goroutine: collects results and writes to file at end
	var results []string
	var writerErr error
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for r := range resultCh {
			results = append(results, r)
		}
		// write to file
		f, err := os.Create(outFile)
		if err != nil {
			writerErr = fmt.Errorf("failed to create output file: %w", err)
			log.Println(writerErr)
			return
		}
		defer f.Close()
		for _, line := range results {
			_, err := f.WriteString(line + "\n")
			if err != nil {
				writerErr = fmt.Errorf("failed to write to file: %w", err)
				log.Println(writerErr)
				return
			}
		}
		log.Println("Writer: results written to", outFile)
	}()

	// start workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("Worker %d started\n", workerID)
			for task := range taskCh {
				// process
				res, err := processTask(workerID, task)
				if err != nil {
					log.Printf("Worker %d error processing %s: %v\n", workerID, task.ID, err)
					continue
				}
				// send result (non-blocking assumption: writer keeps up; if writer slow, this will block)
				resultCh <- res
			}
			log.Printf("Worker %d exiting\n", workerID)
		}(w)
	}

	// produce tasks
	go func() {
		for i := 1; i <= numTasks; i++ {
			taskCh <- Task{ID: fmt.Sprintf("task-%02d", i), Payload: fmt.Sprintf("ride-request-%d", i)}
		}
		close(taskCh) // signal no more tasks
	}()

	// wait for workers to finish then close resultCh
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// wait for writer to finish
	writerWg.Wait()

	if writerErr != nil {
		log.Println("Finished with writer error:", writerErr)
	} else {
		log.Printf("Processing complete. Total results: %d\n", len(results))
	}
}

// processTask simulates work and may return an error sometimes
func processTask(workerID int, t Task) (string, error) {
	// simulate variable processing time
	delay := time.Duration(100+rand.Intn(400)) * time.Millisecond
	time.Sleep(delay)

	// small chance of simulated error
	if rand.Float32() < 0.05 {
		return "", fmt.Errorf("simulated error for %s", t.ID)
	}

	res := fmt.Sprintf("%s | processedBy=worker-%d | at=%s | payload=%s",
		t.ID, workerID, time.Now().Format(time.RFC3339Nano), t.Payload)
	return res, nil
}
