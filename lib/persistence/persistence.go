package persistence

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
)

// Backend hides the underlying implementation of the persistence
type Backend interface {
	// CreateKeyspace should create a keyspace to store data
	CreateKeyspace(
		name, datacenter, contact string,
		ttl time.Duration,
	) gobol.Error

	// DeleteKeyspace should delete a keyspace from the database
	DeleteKeyspace(id string) gobol.Error

	// ListKeyspaces should return a list of all available keyspaces
	ListKeyspaces() ([]Keyspace, gobol.Error)
}

// Storage is a storage for data
type Storage struct {
	logger *logrus.Logger
	Backend
}
