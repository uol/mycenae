package tlmanager

import (
	"time"

	"github.com/uol/timeline"
)

//
// Adds extra shortcut functions to the manager.
// "N" suffix for normal storage
// "A" suffic for archive storage
// @author: rnojiri
//

// FlattenAvgN - calls the Flatten function using normal storage and average operation
func (tm *TimelineManager) FlattenAvgN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Avg, value, metric, tags...)
}

// FlattenCountN - calls the Flatten function using normal storage and count operation
func (tm *TimelineManager) FlattenCountN(caller string, value float64, metric string, tags ...interface{}) {
	if value == 0 {
		return
	}
	tm.Flatten(caller, Normal, timeline.Count, value, metric, tags...)
}

// FlattenCountIncN - calls the Flatten function using normal storage and count operation (adds 1 to the value)
func (tm *TimelineManager) FlattenCountIncN(caller string, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Count, 1, metric, tags...)
}

// FlattenMaxN - calls the Flatten function using normal storage and maximum operation
func (tm *TimelineManager) FlattenMaxN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Max, value, metric, tags...)
}

// FlattenMinN - calls the Flatten function using normal storage and minimum operation
func (tm *TimelineManager) FlattenMinN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Flatten(caller, Normal, timeline.Min, value, metric, tags...)
}

// FlattenCountIncA - calls the Flatten function using archive storage and count operation (adds 1 to the value)
func (tm *TimelineManager) FlattenCountIncA(caller string, metric string, tags ...interface{}) {
	tm.Flatten(caller, Archive, timeline.Count, 1, metric, tags...)
}

// AccumulateCustomHashN - calls the accumulate function using normal storage
func (tm *TimelineManager) AccumulateCustomHashN(hash string) (bool, error) {
	return tm.AccumulateHashedData(Normal, hash)
}

// StoreCustomHashN - calls the store hash function using normal storage
func (tm *TimelineManager) StoreCustomHashN(hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(Normal, hash, tm.configuration.DataTTL.Duration, metric, tags...)
}

// StoreNoTTLCustomHashN - calls the store hash function using normal storage with no ttl
func (tm *TimelineManager) StoreNoTTLCustomHashN(hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(Normal, hash, 0, metric, tags...)
}

// StoreDefaultTTLCustomHash - stores with default configured ttl
func (tm *TimelineManager) StoreDefaultTTLCustomHash(storage StorageType, hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(storage, hash, tm.configuration.DataTTL.Duration, metric, tags...)
}

// GetConfiguredDataTTL - returns the configured data ttl
func (tm *TimelineManager) GetConfiguredDataTTL() time.Duration {
	return tm.configuration.DataTTL.Duration
}
