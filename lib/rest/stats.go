package rest

import tlmanager "github.com/uol/timeline-manager"

// restStatistics - implements the interface rip.Statistics
type restStatistics struct {
	timelineManager *tlmanager.TimelineManager
}

// newRestStatistics - creates a new rest statistics
func newRestStatistics(timelineManager *tlmanager.TimelineManager) *restStatistics {

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
