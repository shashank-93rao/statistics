package main

import (
	"context"
	"github.com/shashank-93rao/statistics/pkg/stats"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Demonstrates working of stats application.
// Just choose the right implementation in getStatsImpl.
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneChan := make(chan bool, 2)

	// Create channel to receive sys interrupts
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
	// In a separate thread, wait for any interruptions
	go func() {
		<-sigChannel
		log.Println("Received SIGTERM")
		// Notify the main thread that the app has to exit
		doneChan <- false
	}()

	// Run the app in a separate thread
	go runApplication(ctx, doneChan)

	<-doneChan // Wait for signal to exit
	cancel()   // Notify any child threads to stop working
	// Wait for some time
	time.Sleep(5 * time.Second)
	log.Println("Done!!")
	// End
}

// Runs the application
func runApplication(ctx context.Context, done chan bool) {
	// Always notify caller on completion
	defer func() { done <- true }()

	totalThreads := 5
	iterationsPerThread := 5
	wg := sync.WaitGroup{}
	wg.Add(totalThreads)

	stats := getStatsImpl(ctx)

	// Start threads to populate and query data
	for i := 0; i < totalThreads; i++ {
		go writeAndQuery(ctx, i+1, iterationsPerThread, stats, &wg)
	}
	// Wait for all the threads to stop
	wg.Wait()

	// Just in case the stats calculator is async
	// wait for the processor to catchup
	time.Sleep(5 * time.Second)

	mean, _ := stats.Mean(ctx)
	variance, _ := stats.Variance(ctx)
	minVal, _ := stats.Min(ctx)
	maxVal, _ := stats.Max(ctx)
	log.Printf("Final Stats: Min: %d, Max: %d, Mean: %f, Variance: %f\n", minVal, maxVal, mean, variance)
}

// Periodically writes events and queries stats
func writeAndQuery(ctx context.Context, id, count int, statistics stats.Statistics, wg *sync.WaitGroup) {
	// Always notify the caller that the function is complete at the end
	defer wg.Done()

	for i := 0; i < count; i++ {
		select {
		// if the caller chain cancels
		case _ = <-ctx.Done():
			log.Printf("Thread %d exiting\n", id)
			return // exit
		default: // else write and read
			time.Sleep(time.Duration((time.Now().UnixMilli()%3)+1) * time.Second)
			log.Printf("Thread %d woke up to write: %d\n", id, i+1)
			_ = statistics.Event(ctx, int32(i+1))
			mean, _ := statistics.Mean(ctx)
			variance, _ := statistics.Variance(ctx)
			minVal, _ := statistics.Min(ctx)
			maxVal, _ := statistics.Max(ctx)
			log.Printf("Thread %d. Stats: Min: %d, Max: %d, Mean: %f, Variance: %f\n", id, minVal, maxVal, mean, variance)
		}
	}
	// exit on completion
}

func getStatsImpl(ctx context.Context) stats.Statistics {
	return stats.NewChannelBasedStats(ctx)
}
