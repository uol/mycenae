package timeline

import (
	"fmt"
	"time"

	jsonSerializer "github.com/uol/serializer/json"
	openTSDBSerializer "github.com/uol/serializer/opentsdb"
)

/**
* Implements functions related with sending single items.
* @author rnojiri
**/

// Send - sends a new data using the current transport
func (m *Manager) Send(genericItem interface{}) {

	m.transport.DataChannel(genericItem)
}

// SendJSON - sends a new data using the json transport
func (m *Manager) SendJSON(schemaName string, parameters ...interface{}) error {

	if !m.transport.MatchType(typeHTTP) && !m.transport.MatchType(typeUDP) {
		return fmt.Errorf("this transport does not accepts json messages")
	}

	m.transport.DataChannel(
		&jsonSerializer.ArrayItem{
			Name:       schemaName,
			Parameters: parameters,
		},
	)

	return nil
}

// SendOpenTSDB - sends a new data using the openTSDB transport
func (m *Manager) SendOpenTSDB(value float64, timestamp int64, metric string, tags ...interface{}) error {

	if !m.transport.MatchType(typeOpenTSDB) {
		return fmt.Errorf("this transport does not accepts opentsdb messages")
	}

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	m.transport.DataChannel(&openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	})

	return nil
}
