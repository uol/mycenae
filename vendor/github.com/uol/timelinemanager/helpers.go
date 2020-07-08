package timelinemanager

import (
	"github.com/uol/timeline"
)

//
// Adds extra shortcut functions to the manager.
// "N" suffix for normal storage
// "A" suffic for archive storage
// @author: rnojiri
//

// FlattenAvgN - calls the Flatten function using normal storage and average operation
func (tm *Instance) FlattenAvgN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Send(caller, NormalStorage, timeline.Avg, value, metric, tags...)
}

// FlattenCountN - calls the Flatten function using normal storage and count operation
func (tm *Instance) FlattenCountN(caller string, value float64, metric string, tags ...interface{}) {
	if value == 0 {
		return
	}
	// WARNING!!! this operation must be sum because count only sums the number of occurrences and not their values
	tm.Send(caller, NormalStorage, timeline.Sum, value, metric, tags...)
}

// FlattenCountIncN - calls the Flatten function using normal storage and count operation (adds 1 to the value)
func (tm *Instance) FlattenCountIncN(caller string, metric string, tags ...interface{}) {
	tm.Send(caller, NormalStorage, timeline.Count, 1, metric, tags...)
}

// FlattenMaxN - calls the Flatten function using normal storage and maximum operation
func (tm *Instance) FlattenMaxN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Send(caller, NormalStorage, timeline.Max, value, metric, tags...)
}

// FlattenMinN - calls the Flatten function using normal storage and minimum operation
func (tm *Instance) FlattenMinN(caller string, value float64, metric string, tags ...interface{}) {
	tm.Send(caller, NormalStorage, timeline.Min, value, metric, tags...)
}

// FlattenCountIncA - calls the Flatten function using archive storage and count operation (adds 1 to the value)
func (tm *Instance) FlattenCountIncA(caller string, metric string, tags ...interface{}) {
	tm.Send(caller, ArchiveStorage, timeline.Count, 1, metric, tags...)
}

// AccumulateCustomHashN - calls the accumulate function using normal storage
func (tm *Instance) AccumulateCustomHashN(hash string) (bool, error) {
	return tm.AccumulateHashedData(NormalStorage, hash)
}

// StoreCustomHashN - calls the store hash function using normal storage
func (tm *Instance) StoreCustomHashN(hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(NormalStorage, hash, tm.configuration.DataTTL.Duration, metric, tags...)
}

// StoreNoTTLCustomHashN - calls the store hash function using normal storage with no ttl
func (tm *Instance) StoreNoTTLCustomHashN(hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(NormalStorage, hash, 0, metric, tags...)
}

// StoreDefaultTTLCustomHash - stores with default configured ttl
func (tm *Instance) StoreDefaultTTLCustomHash(storage StorageType, hash string, metric string, tags ...interface{}) error {
	return tm.StoreHashedData(storage, hash, tm.configuration.DataTTL.Duration, metric, tags...)
}
