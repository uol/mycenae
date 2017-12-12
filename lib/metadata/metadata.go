package metadata

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
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
	logger *logrus.Logger

	// Backend is the thing that actually does the specific work in the storage
	Backend
}
