package factory

import (
	"context"
	"errors"
	"github.com/shashank-93rao/statistics/pkg/stats"
	"github.com/shashank-93rao/statistics/pkg/stats/async/chbased"
	"github.com/shashank-93rao/statistics/pkg/stats/async/lockbased"
)

// StatsType is enum of various stats implementation
type StatsType string

const (
	CH StatsType = "CH"
	LB StatsType = "LB"
)

func GetStats(ctx context.Context, tp StatsType) (s stats.Statistics, err error) {
	switch tp {
	case LB:
		s = lockbased.NewStats(ctx)
		break
	case CH:
		s = chbased.NewStats(ctx)
		break
	default:
		err = errors.New("Unknown stats calculator")
	}
	return
}
