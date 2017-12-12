package persistence

import (
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uol/gobol"
)

// Backend hides the underlying implementation of the persistence
type Backend interface {
	// CreateKeyspace should create a keyspace to store data
	CreateKeyspace(
		ksid, name, datacenter, contact string,
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

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// GenerateKeyspaceIdentifier generates the unique ID for keyspaces
func GenerateKeyspaceIdentifier() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}
