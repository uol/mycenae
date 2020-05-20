package memcached

import (
	"github.com/uol/mycenae/lib/stats"
)

const (
	countFuncName string = "memcached.Count"
	maxFuncName   string = "memcached.Maximum"
)

// metricsCollector - implements the interface zencached.MetricsCollector
type metricsCollector struct {

	// must be replaced with timeline
	timelineManager *stats.TimelineManager
}

// newMetricsCollector - creates a new metrics collector for memcached
func newMetricsCollector(timelineManager *stats.TimelineManager) *metricsCollector {

	return &metricsCollector{
		timelineManager: timelineManager,
	}
}

// Count - does the count operation
func (mc *metricsCollector) Count(value float64, metric string, tags ...interface{}) {

	mc.timelineManager.FlattenCountN(countFuncName, value, metric, tags...)
}

// Maximum - does the max operation
func (mc *metricsCollector) Maximum(value float64, metric string, tags ...interface{}) {

	mc.timelineManager.FlattenMaxN(maxFuncName, value, metric, tags...)
}
