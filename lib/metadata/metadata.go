package metadata

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
)

// Backend hides the underlying implementation of the metadata storage
type Backend interface {
	CreateIndex() gobol.Error
	DeleteIndex() gobol.Error
}

// Storage is a storage for metadata
type Storage struct {
	logger  *logrus.Logger
	backend Backend
}
