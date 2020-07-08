package timeline

import (
	"fmt"

	jsonSerializer "github.com/uol/serializer/json"
	openTSDBSerializer "github.com/uol/serializer/opentsdb"
)

/**
* Implements functions related with flattening.
* @author rnojiri
**/

// Flatten - flatten a point
func (m *Manager) Flatten(operation FlatOperation, genericItem interface{}) error {

	point, err := m.transport.DataChannelItemToFlattenerPoint(
		m.flattener.configuration,
		genericItem,
		operation,
	)

	if err != nil {
		return err
	}

	return m.flattener.Add(point.(*FlattenerPoint))
}

// FlattenJSON - flatten a point
func (m *Manager) FlattenJSON(operation FlatOperation, name string, parameters ...interface{}) error {

	if !m.transport.MatchType(typeHTTP) && !m.transport.MatchType(typeUDP) {
		return fmt.Errorf("this transport does not accepts json messages")
	}

	return m.Flatten(
		operation,
		&jsonSerializer.ArrayItem{
			Name:       name,
			Parameters: parameters,
		},
	)
}

// FlattenOpenTSDB - flatten a point
func (m *Manager) FlattenOpenTSDB(operation FlatOperation, value float64, timestamp int64, metric string, tags ...interface{}) error {

	if !m.transport.MatchType(typeOpenTSDB) {
		return fmt.Errorf("this transport does not accepts opentsdb messages")
	}

	return m.Flatten(
		operation,
		&openTSDBSerializer.ArrayItem{
			Metric:    metric,
			Tags:      tags,
			Timestamp: timestamp,
			Value:     value,
		},
	)
}
