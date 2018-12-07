package persistence

import (
	"strings"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

// Backend hides the underlying implementation of the persistence
type Backend interface {
	// CreateKeyspace should create a keyspace to store data
	CreateKeyspace(
		name, datacenter, contact string,
		replication int, ttl int,
	) gobol.Error
	// DeleteKeyspace should delete a keyspace from the database
	DeleteKeyspace(id string) gobol.Error
	// ListKeyspaces should return a list of all available keyspaces
	ListKeyspaces() ([]Keyspace, gobol.Error)
	// GetKeyspace should return the management data regarding the keyspace
	GetKeyspace(id string) (Keyspace, bool, gobol.Error)
	// UpdateKeyspace should update metadata and contact information about the
	// keyspace
	UpdateKeyspace(ksid, contact string) gobol.Error

	// ListDatacenters should list all available datacenters
	ListDatacenters() ([]string, gobol.Error)
}

// Storage is a storage for data
type Storage struct {
	logger   *zap.Logger
	metadata *metadata.Storage

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// NewStorage creates a new storage persistence
func NewStorage(
	ksAdmin string,
	grantUser string,
	logger *zap.Logger,
	session *gocql.Session,
	metadata *metadata.Storage,
	stats *tsstats.StatsTS,
	devMode bool,
	defaultTTL int,
) (*Storage, error) {
	backend, err := newScyllaPersistence(
		ksAdmin, grantUser, session, logger, stats, devMode, defaultTTL,
	)
	if err != nil {
		return nil, err
	}
	return &Storage{
		logger:   logger,
		metadata: metadata,
		Backend:  backend,
	}, nil
}

// GenerateKeyspaceIdentifier generates the unique ID for keyspaces
func GenerateKeyspaceIdentifier() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}
