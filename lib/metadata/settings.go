package metadata

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
)

// Settings configures the metadata storage
type Settings struct {
}

// Create creates a metadata handler
func Create(
	settings Settings,
	logger *logrus.Logger,
) (*Storage, gobol.Error) {
	return &Storage{
		logger: logger,

		// TODO: fixme
		Backend: nil,
	}, nil
}
