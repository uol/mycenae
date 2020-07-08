package timeline

import (
	"time"

	jsonSerializer "github.com/uol/serializer/json"
	openTSDBSerializer "github.com/uol/serializer/opentsdb"
)

/**
* Implements functions related with accumulation.
* @author rnojiri
**/

// StoreDataToAccumulateJSON - stores a data to accumulate
func (m *Manager) StoreDataToAccumulateJSON(ttl time.Duration, name string, parameters ...interface{}) (string, error) {

	return m.StoreDataToAccumulate(
		ttl,
		&jsonSerializer.ArrayItem{
			Name:       name,
			Parameters: parameters,
		},
	)
}

// StoreDataToAccumulate - stores a data to accumulate
func (m *Manager) StoreDataToAccumulate(ttl time.Duration, genericItem interface{}) (string, error) {

	return m.accumulator.Store(genericItem, ttl)
}

// StoreHashedDataToAccumulateJSON - stores a data with custom hash to accumulate
func (m *Manager) StoreHashedDataToAccumulateJSON(hash string, ttl time.Duration, name string, parameters ...interface{}) error {

	return m.accumulator.StoreCustomHash(&jsonSerializer.ArrayItem{
		Name:       name,
		Parameters: parameters,
	}, ttl, hash)
}

// StoreDataToAccumulateOpenTSDB - stores a data to accumulate
func (m *Manager) StoreDataToAccumulateOpenTSDB(ttl time.Duration, value float64, timestamp int64, metric string, tags ...interface{}) (string, error) {

	return m.accumulator.Store(&openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	}, ttl)
}

// StoreHashedDataToAccumulateOpenTSDB - stores a data with custom hash to accumulate
func (m *Manager) StoreHashedDataToAccumulateOpenTSDB(hash string, ttl time.Duration, value float64, timestamp int64, metric string, tags ...interface{}) error {

	return m.accumulator.StoreCustomHash(&openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	}, ttl, hash)
}

// IncrementAccumulatedData - stores a data to accumulate
func (m *Manager) IncrementAccumulatedData(hash string) error {

	return m.accumulator.Add(hash)
}
