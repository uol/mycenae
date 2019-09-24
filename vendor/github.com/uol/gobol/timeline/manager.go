package timeline

import "fmt"

// Manager - the parent of all event managers
type Manager struct {
	defaultTags map[string]string
	transport   Transport
}

// NewManager - creates a timeline manager
func NewManager(transport Transport, backend *Backend, defaultTags map[string]string) (*Manager, error) {

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

	if defaultTags == nil {

		defaultTags = map[string]string{}
	}

	return &Manager{
		transport:   transport,
		defaultTags: defaultTags,
	}, nil
}

// SendNumberPoint - sends a number point
func (m *Manager) SendNumberPoint(point *NumberPoint) error {

	if point == nil {
		return fmt.Errorf("number point is null")
	}

	if len(m.defaultTags) > 0 {

		for t, v := range m.defaultTags {

			point.Tags[t] = v
		}
	}

	m.transport.PointChannel() <- point

	return nil
}

// SendTextPoint - sends a text point
func (m *Manager) SendTextPoint(point *TextPoint) error {

	if point == nil {
		return fmt.Errorf("texty point is null")
	}

	if len(m.defaultTags) > 0 {

		for t, v := range m.defaultTags {

			point.Tags[t] = v
		}
	}

	m.transport.PointChannel() <- point

	return nil
}

// Shutdown - shuts down the transport
func (m *Manager) Shutdown() {

	m.transport.Close()
}
