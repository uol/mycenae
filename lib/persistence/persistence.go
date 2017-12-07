package persistence

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
)

// Backend hides the underlying implementation of the persistence
type Backend interface {
	// CreateKeyspace should create a keyspace to store data
	CreateKeyspace() gobol.Error
	// DeleteKeyspace should
	DeleteKeyspace() gobol.Error
}

// Storage is a storage for data
type Storage struct {
	logger  *logrus.Logger
	backend Backend
}
