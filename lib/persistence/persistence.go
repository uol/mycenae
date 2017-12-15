package persistence

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
)

// Backend hides the underlying implementation of the persistence
type Backend interface {
	// CreateKeyspace should create a keyspace to store data
	CreateKeyspace(ksid, name, datacenter, contact string, ttl int) gobol.Error

	// DeleteKeyspace should delete a keyspace from the database
	DeleteKeyspace(id string) gobol.Error

	// ListKeyspaces should return a list of all available keyspaces
	ListKeyspaces() ([]Keyspace, gobol.Error)

	// GetKeyspace should return the management data regarding the keyspace
	GetKeyspace(id string) (Keyspace, bool, gobol.Error)
}

// Storage is a storage for data
type Storage struct {
	logger   *logrus.Logger
	metadata *metadata.Storage

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// NewStorage creates a new storage persistence
func NewStorage(
	logger *logrus.Logger, metadata *metadata.Storage,
) (*Storage, error) {
	return &Storage{
		logger:   logger,
		metadata: metadata,
		Backend:  nil,
	}, nil
}

// GenerateKeyspaceIdentifier generates the unique ID for keyspaces
func GenerateKeyspaceIdentifier() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}

// CreateKeyspace is a wrapper around the Backend in order to create metadata
// with the actual keyspace creation
func (storage *Storage) CreateKeyspace(
	ksid, name, datacenter, contact string, ttl int,
) gobol.Error {
	if err := storage.metadata.CreateIndex(ksid); err != nil {
		return err
	}
	if err := storage.Backend.CreateKeyspace(
		ksid, name, datacenter, contact, ttl,
	); err != nil {
		return err
	}
	return nil
}

// DeleteKeyspace is a wrapper around keyspace deletion to ensure the metadata
// is deleted with the keyspace
func (storage *Storage) DeleteKeyspace(id string) gobol.Error {
	if err := storage.metadata.DeleteIndex(id); err != nil {
		return err
	}
	if err := storage.Backend.DeleteKeyspace(id); err != nil {
		return err
	}
	return nil
}
