package timeline

import (
	"fmt"
	"time"

	jsonSerializer "github.com/uol/serializer/json"
	openTSDBSerializer "github.com/uol/serializer/opentsdb"
)

/**
* Manages the transport and backend configuration.
* @author rnojiri
**/

// Manager - the parent of all event managers
type Manager struct {
	transport   Transport
	flattener   *Flattener
	accumulator *Accumulator
}

// Backend - the destiny opentsdb backend
type Backend struct {
	Host string
	Port int
}

// NewManager - creates a timeline manager
func NewManager(transport Transport, flattener, accumulator DataProcessor, backend *Backend) (*Manager, error) {

	if transport == nil {
		return nil, fmt.Errorf("transport implementation is required")
	}

	if backend == nil {
		return nil, fmt.Errorf("no backend configuration was found")
	}

	err := transport.ConfigureBackend(backend)
	if err != nil {
		return nil, err
	}

	var f *Flattener
	if flattener != nil {
		flattener.SetTransport(transport)
		f = flattener.(*Flattener)
	}

	var a *Accumulator
	if accumulator != nil {
		accumulator.SetTransport(transport)
		a = accumulator.(*Accumulator)
	}

	return &Manager{
		transport:   transport,
		flattener:   f,
		accumulator: a,
	}, nil
}

// SendHTTP - sends a new data using the http transport
func (m *Manager) SendHTTP(schemaName string, parameters ...interface{}) error {

	if !m.transport.MatchType(typeHTTP) {
		return fmt.Errorf("this transport does not accepts http messages")
	}

	m.transport.DataChannel() <- &jsonSerializer.ArrayItem{
		Name:       schemaName,
		Parameters: parameters,
	}

	return nil
}

// SerializeHTTP - serializes a point using the json serializer
func (m *Manager) SerializeHTTP(schemaName string, parameters ...interface{}) (string, error) {

	return m.transport.Serialize(&jsonSerializer.ArrayItem{
		Name:       schemaName,
		Parameters: parameters,
	})
}

// FlattenHTTP - flatten a point
func (m *Manager) FlattenHTTP(operation FlatOperation, name string, parameters ...interface{}) error {

	point, err := m.transport.DataChannelItemToFlattenerPoint(
		m.flattener.configuration,
		&jsonSerializer.ArrayItem{
			Name:       name,
			Parameters: parameters,
		},
		operation,
	)

	if err != nil {
		return err
	}

	return m.flattener.Add(point.(*FlattenerPoint))
}

// SendOpenTSDB - sends a new data using the openTSDB transport
func (m *Manager) SendOpenTSDB(value float64, timestamp int64, metric string, tags ...interface{}) error {

	if !m.transport.MatchType(typeOpenTSDB) {
		return fmt.Errorf("this transport does not accepts opentsdb messages")
	}

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	m.transport.DataChannel() <- &openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	}

	return nil
}

// SerializeOpenTSDB - serializes a point using the opentsdb serializer
func (m *Manager) SerializeOpenTSDB(value float64, timestamp int64, metric string, tags ...interface{}) (string, error) {

	return m.transport.Serialize(&openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	})
}

// FlattenOpenTSDB - flatten a point
func (m *Manager) FlattenOpenTSDB(operation FlatOperation, value float64, timestamp int64, metric string, tags ...interface{}) error {

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	point, err := m.transport.DataChannelItemToFlattenerPoint(
		m.flattener.configuration,
		&openTSDBSerializer.ArrayItem{
			Metric:    metric,
			Tags:      tags,
			Timestamp: timestamp,
			Value:     value,
		},
		operation,
	)

	if err != nil {
		return err
	}

	return m.flattener.Add(point.(*FlattenerPoint))
}

// StoreDataToAccumulateHTTP - stores a data to accumulate
func (m *Manager) StoreDataToAccumulateHTTP(ttl time.Duration, name string, parameters ...interface{}) (string, error) {

	return m.accumulator.Store(&jsonSerializer.ArrayItem{
		Name:       name,
		Parameters: parameters,
	}, ttl)
}

// StoreHashedDataToAccumulateHTTP - stores a data with custom hash to accumulate
func (m *Manager) StoreHashedDataToAccumulateHTTP(hash string, ttl time.Duration, name string, parameters ...interface{}) error {

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

// Start - starts the manager
func (m *Manager) Start() error {

	err := m.transport.Start()
	if err != nil {
		return err
	}

	if m.flattener != nil {
		m.flattener.Start()
	}

	if m.accumulator != nil {
		m.accumulator.Start()
	}

	return nil
}

// Shutdown - shuts down the transport
func (m *Manager) Shutdown() {

	if m.flattener != nil {
		m.flattener.Stop()
		return
	}

	if m.accumulator != nil {
		m.accumulator.Stop()
		return
	}

	m.transport.Close()
}

// GetTransport - returns the configured transport
func (m *Manager) GetTransport() Transport {

	return m.transport
}
