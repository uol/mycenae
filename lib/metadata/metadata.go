package metadata

import (
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

// Backend hides the underlying implementation of the metadata storage
type Backend interface {
	// CreateIndex creates indexes in the metadata storage
	CreateIndex(name string) gobol.Error

	// DeleteIndex deletes the index in the metadata storage
	DeleteIndex(name string) gobol.Error
}

// Storage is a storage for metadata
type Storage struct {
	logger *zap.Logger

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// Create creates a metadata handler
func Create(
	settings rubber.Settings,
	logger *zap.Logger,
	stats *tsstats.StatsTS,
) (*Storage, error) {
	backend, err := newElasticBackend(logger, stats, settings)
	if err != nil {
		return nil, err
	}
	return &Storage{
		logger:  logger,
		Backend: backend,
	}, nil
}
