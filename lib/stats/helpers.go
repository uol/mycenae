package stats

import "github.com/uol/timeline"

//
// Adds extra shortcut functions to the manager.
// @author: rnojiri
//

// FlattenAvgN - calls the normal Flatten function using normal storage and average operation
func (tm *TimelineManager) FlattenAvgN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Avg, value, metric, tags...)
}

// FlattenCountN - calls the normal Flatten function using normal storage and count operation
func (tm *TimelineManager) FlattenCountN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Count, value, metric, tags...)
}

// FlattenCountIncN - calls the normal Flatten function using normal storage and count operation (adds 1 to the value)
func (tm *TimelineManager) FlattenCountIncN(caller string, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Count, 1, metric, tags...)
}

// FlattenMaxN - calls the normal Flatten function using normal storage and maximum operation
func (tm *TimelineManager) FlattenMaxN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Max, value, metric, tags...)
}

// FlattenMinN - calls the normal Flatten function using normal storage and minimum operation
func (tm *TimelineManager) FlattenMinN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Min, value, metric, tags...)
}

// FlattenCountIncA - calls the normal Flatten function using archive storage and count operation (adds 1 to the value)
func (tm *TimelineManager) FlattenCountIncA(caller string, metric string, tags ...interface{}) {
	tm.Flatten(caller, Archive, timeline.Count, 1, metric, tags...)
}
