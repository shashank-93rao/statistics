package statistics

import "context"

type Statistics interface {
	Event(ctx context.Context, n int32) error

	Min(ctx context.Context) (int32, error)

	Max(ctx context.Context) (int32, error)

	Mean(ctx context.Context) (float64, error)

	Variance(ctx context.Context) (float64, error)
}
