package rest

import (
	"github.com/uol/mycenae/lib/stats"
)

// restStatistics - implements the interface rip.Statistics
type restStatistics struct {
	timelineManager *stats.TimelineManager
}

// newRestStatistics - creates a new rest statistics
func newRestStatistics(timelineManager *stats.TimelineManager) *restStatistics {

	return &restStatistics{
		timelineManager: timelineManager,
	}
}

const funcIncrement string = "Increment"

// Increment - increments a metric
func (rs *restStatistics) Increment(metric string, tags ...interface{}) {

	rs.timelineManager.FlattenCountIncN(funcIncrement, metric, tags...)
}

const funcMaximum string = "Maximum"

// Maximum - input a maximum operation
func (rs *restStatistics) Maximum(metric string, value float64, tags ...interface{}) {

	rs.timelineManager.FlattenMaxN(funcMaximum, value, metric, tags...)
}
