package lockbased

// Async event push with lock based computation

import (
	"context"
	"log"
	"math"
	"sync"

	"github.com/shashank-93rao/statistics/pkg/stats"
)

// Holds the values computed by the stats calculator
type statsValues struct {
	sum    float64
	sqrSum float64
	count  float64

	mean     float64
	variance float64
	min      int32
	max      int32
}

// Embeds the statsValues.
// Also holds the synchronization mechanisms
type channelBasedStats struct {
	statsValues

	lock      sync.RWMutex
	eventChan chan int32
}

// Event Takes in an integer and computes the statistics. Statistics
// is computed asynchronously. There is no guarantee provided that
// making a read immediately after a write will reflect the latest
// state of the system. Hence, this is an eventually consistent system.
// This is not a blocking call unless the no.of writes is so huge
// that the computation thread is unable to keep up.
func (stats *channelBasedStats) Event(ctx context.Context, n int32) error {
	log.Printf("Writing event: %d\n", n)
	stats.eventChan <- n
	log.Printf("Event %d pushed\n", n)
	return nil
}

// Min returns the last computed min element.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Min(ctx context.Context) (int32, error) {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.min, nil
}

// Max returns the last computed max element.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Max(ctx context.Context) (int32, error) {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.max, nil
}

// Mean returns the last computed mean of the system.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Mean(ctx context.Context) (float64, error) {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.mean, nil
}

// Variance returns the last computed variance of the system.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Variance(ctx context.Context) (float64, error) {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.variance, nil
}

// NewStats returns a statistics calculator which asynchronously
// computes the statistics for every event added into the system. Calling this
// function also starts an asynchronous goroutine which computes the incremental
// statistics for every event added into the system. It is very important that
// the context passed as an argument is closed at the end. Failure to do so will
// leave the computation thread dangling.
func NewStats(ctx context.Context) stats.Statistics {
	statsObj := &channelBasedStats{
		lock:      sync.RWMutex{},
		eventChan: make(chan int32, 100),
		statsValues: statsValues{
			sum:    0,
			sqrSum: 0,
			count:  0,

			mean:     0,
			variance: 0,
			min:      math.MaxInt32,
			max:      math.MinInt32,
		},
	}
	go runComputeThread(ctx, statsObj)
	return statsObj
}

// sets the statistics after obtaining the write lock
func setStats(stats *channelBasedStats, event int32, values statsValues) {
	stats.lock.Lock()
	defer stats.lock.Unlock()

	log.Printf("Received event num %.0f, %d. Stats: Min: %d, Max: %d, Mean: %f, Variance: %f\n", values.count, event, values.min, values.max, values.mean, values.variance)

	stats.min = values.min
	stats.max = values.max
	stats.mean = values.mean
	stats.variance = values.variance
	stats.count = values.count
	stats.sum = values.sum
	stats.sqrSum = values.sqrSum
}

// Starts the computation thread
func runComputeThread(ctx context.Context, stats *channelBasedStats) {
	log.Println("Starting the compute thread")
	for {
		select {
		case _ = <-ctx.Done(): // If caller chain cancelled
			log.Println("Context cancelled... Stopping compute thread")
			// Close the channel so that any writes will now panic
			close(stats.eventChan)
			return
		case event := <-stats.eventChan: // if there are any events
			computeAndSetStats(stats, event)

		}
	}
}

// compute the stats
func computeAndSetStats(stats *channelBasedStats, event int32) {
	sum := stats.sum + float64(event)
	sqrSum := stats.sqrSum + math.Pow(float64(event), 2)
	count := stats.count + 1

	mean := sum / count
	//Proof: https: //math.stackexchange.com/a/1379804
	variance := (sqrSum / count) - math.Pow(sum/count, 2)
	minVal := minInt(stats.min, event)
	maxVal := maxInt(stats.max, event)

	values := statsValues{
		sum:      sum,
		sqrSum:   sqrSum,
		count:    count,
		mean:     mean,
		variance: variance,
		min:      minVal,
		max:      maxVal,
	}
	setStats(stats, event, values)
}

func minInt(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int32) int32 {
	if a < b {
		return b
	}
	return a
}
