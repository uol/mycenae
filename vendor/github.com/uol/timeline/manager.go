package timeline

import (
	"fmt"
)

/**
* Manages the transport and backend configuration.
* @author rnojiri
**/

const logContextID string = "id"

// Manager - the parent of all event managers
type Manager struct {
	transport   Transport
	flattener   *Flattener
	accumulator *Accumulator
	name        string
}

// NewManager - creates a timeline manager
func NewManager(transport Transport, flattener, accumulator DataProcessor, backend *Backend, customContext ...string) (*Manager, error) {

	if transport == nil {
		return nil, fmt.Errorf("transport implementation is required")
	}

	if backend == nil {
		return nil, fmt.Errorf("no backend configuration was found")
	}

	loggerContext := []string{logContextID, fmt.Sprintf("%s:%d", backend.Host, backend.Port)}

	if len(customContext) > 0 {
		loggerContext = append(loggerContext, customContext...)
	}

	transport.BuildContextualLogger(loggerContext...)

	err := transport.ConfigureBackend(backend)
	if err != nil {
		return nil, err
	}

	var f *Flattener
	if flattener != nil {
		flattener.BuildContextualLogger(loggerContext...)
		flattener.SetTransport(transport)
		f = flattener.(*Flattener)
	}

	var a *Accumulator
	if accumulator != nil {
		accumulator.BuildContextualLogger(loggerContext...)
		accumulator.SetTransport(transport)
		a = accumulator.(*Accumulator)
	}

	return &Manager{
		transport:   transport,
		flattener:   f,
		accumulator: a,
	}, nil
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
