package timeline

import (
	jsonSerializer "github.com/uol/serializer/json"
	openTSDBSerializer "github.com/uol/serializer/opentsdb"
)

/**
* Implements functions related with serialization.
* @author rnojiri
**/

// Serialize - serializes a point using the json serializer
func (m *Manager) Serialize(genericItem interface{}) (string, error) {

	return m.transport.Serialize(genericItem)
}

// SerializeJSON - serializes a point using the json serializer
func (m *Manager) SerializeJSON(schemaName string, parameters ...interface{}) (string, error) {

	return m.Serialize(
		&jsonSerializer.ArrayItem{
			Name:       schemaName,
			Parameters: parameters,
		},
	)
}

// SerializeOpenTSDB - serializes a point using the opentsdb serializer
func (m *Manager) SerializeOpenTSDB(value float64, timestamp int64, metric string, tags ...interface{}) (string, error) {

	return m.Serialize(&openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	})
}
