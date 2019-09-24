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
	transport Transport
	flattener *Flattener
}

// Backend - the destiny opentsdb backend
type Backend struct {
	Host string
	Port int
}

// NewManager - creates a timeline manager
func NewManager(transport Transport, backend *Backend) (*Manager, error) {

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

	return &Manager{
		transport: transport,
	}, nil
}

// NewManagerF - creates a timeline manager with flattener
func NewManagerF(flattener *Flattener, backend *Backend) (*Manager, error) {

	if flattener == nil {
		return nil, fmt.Errorf("flattener implementation is required")
	}

	if backend == nil {
		return nil, fmt.Errorf("no backend configuration was found")
	}

	err := flattener.transport.ConfigureBackend(backend)
	if err != nil {
		return nil, err
	}

	return &Manager{
		flattener: flattener,
		transport: flattener.transport,
	}, nil
}

// SendHTTP - sends a new data using the http transport
func (m *Manager) SendHTTP(schemaName string, parameters ...interface{}) error {

	if !m.transport.MatchType(typeHTTP) {
		return fmt.Errorf("this transport does not accepts http messages")
	}

	m.transport.DataChannel() <- jsonSerializer.ArrayItem{
		Name:       schemaName,
		Parameters: parameters,
	}

	return nil
}

// FlattenHTTP - flatten a point
func (m *Manager) FlattenHTTP(operation FlatOperation, name string, parameters ...interface{}) error {

	flattenerPoint, err := m.transport.DataChannelItemToFlattenedPoint(
		operation,
		&jsonSerializer.ArrayItem{
			Name:       name,
			Parameters: parameters,
		},
	)

	if err != nil {
		return err
	}

	return m.flattener.Add(flattenerPoint)
}

// SendOpenTSDB - sends a new data using the openTSDB transport
func (m *Manager) SendOpenTSDB(value float64, timestamp int64, metric string, tags ...interface{}) error {

	if !m.transport.MatchType(typeOpenTSDB) {
		return fmt.Errorf("this transport does not accepts opentsdb messages")
	}

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	m.transport.DataChannel() <- openTSDBSerializer.ArrayItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	}

	return nil
}

// FlattenOpenTSDB - flatten a point
func (m *Manager) FlattenOpenTSDB(operation FlatOperation, value float64, timestamp int64, metric string, tags ...interface{}) error {

	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	flattenerPoint, err := m.transport.DataChannelItemToFlattenedPoint(
		operation,
		&openTSDBSerializer.ArrayItem{
			Metric:    metric,
			Tags:      tags,
			Timestamp: timestamp,
			Value:     value,
		},
	)

	if err != nil {
		return err
	}

	return m.flattener.Add(flattenerPoint)
}

// Start - starts the manager
func (m *Manager) Start() error {

	if m.flattener != nil {
		return m.flattener.Start()
	}

	return m.transport.Start()
}

// Shutdown - shuts down the transport
func (m *Manager) Shutdown() {

	if m.flattener != nil {
		m.flattener.Close()

		return
	}

	m.transport.Close()
}

// GetTransport - returns the configured transport
func (m *Manager) GetTransport() Transport {

	return m.transport
}
