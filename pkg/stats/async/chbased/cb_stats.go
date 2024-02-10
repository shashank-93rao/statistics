package chbased

// Async event push with channel based request dispatcher

import (
	"context"
	"github.com/shashank-93rao/statistics/pkg/stats"
	"log"
	"math"
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

type answerChan chan statsValues

// Embeds the statsValues.
// Also holds the communication channels
type channelBasedStats struct {
	eventChan chan int32
	reqChan   chan answerChan
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
	responseChan := make(answerChan, 1)
	stats.reqChan <- responseChan // Send request
	response := <-responseChan    // Wait for response
	return response.min, nil
}

// Max returns the last computed max element.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Max(ctx context.Context) (int32, error) {
	responseChan := make(answerChan, 1)
	stats.reqChan <- responseChan // Send request
	response := <-responseChan    // Wait for response
	return response.max, nil
}

// Mean returns the last computed mean of the system.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Mean(ctx context.Context) (float64, error) {
	responseChan := make(answerChan, 1)
	stats.reqChan <- responseChan // Send request
	response := <-responseChan    // Wait for response
	return response.mean, nil
}

// Variance returns the last computed variance of the system.
// It may or may not reflect the state after the latest write.
func (stats *channelBasedStats) Variance(ctx context.Context) (float64, error) {
	responseChan := make(answerChan, 1)
	stats.reqChan <- responseChan // Send request
	response := <-responseChan    // Wait for response
	return response.variance, nil
}

// NewStats returns a statistics calculator which asynchronously
// computes the statistics for every event added into the system. Calling this
// function also starts an asynchronous goroutine which computes the incremental
// statistics for every event added into the system. It is very important that
// the context passed as an argument is closed at the end. Failure to do so will
// leave the computation thread dangling.
func NewStats(ctx context.Context) stats.Statistics {
	statsObj := &channelBasedStats{
		reqChan:   make(chan answerChan, 100),
		eventChan: make(chan int32, 100),
	}
	go runDispatcherThread(ctx, statsObj)
	return statsObj
}

// Starts the computation thread
func runDispatcherThread(ctx context.Context, stats *channelBasedStats) {
	log.Println("Starting the compute thread")
	curr := statsValues{
		sum:    0,
		sqrSum: 0,
		count:  0,

		mean:     0,
		variance: 0,
		min:      math.MaxInt32,
		max:      math.MinInt32,
	}
	for {
		select {
		case _ = <-ctx.Done(): // If caller chain cancelled
			log.Println("Context cancelled... Stopping compute thread")
			// Close the channel so that any writes will now panic
			close(stats.eventChan)
			return
		case event := <-stats.eventChan: // if there are any events
			curr = computeStats(curr, event)
		case respChan := <-stats.reqChan:
			respChan <- curr
		}
	}
}

// compute the stats
func computeStats(current statsValues, event int32) statsValues {
	sum := current.sum + float64(event)
	sqrSum := current.sqrSum + math.Pow(float64(event), 2)
	count := current.count + 1

	mean := sum / count
	//Proof: https: //math.stackexchange.com/a/1379804
	variance := (sqrSum / count) - math.Pow(sum/count, 2)
	minVal := minInt(current.min, event)
	maxVal := maxInt(current.max, event)

	return statsValues{
		sum:      sum,
		sqrSum:   sqrSum,
		count:    count,
		mean:     mean,
		variance: variance,
		min:      minVal,
		max:      maxVal,
	}
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
