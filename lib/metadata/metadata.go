package metadata

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

// Backend hides the underlying implementation of the metadata storage
type Backend interface {
	// CreateIndex creates indexes in the metadata storage
	CreateIndex(name string) gobol.Error

	// DeleteIndex deletes the index in the metadata storage
	DeleteIndex(name string) gobol.Error

	// FilterTagValues - filter tag values from a collection
	FilterTagValues(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterTagKeys - filter tag keys from a collection
	FilterTagKeys(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterMetrics - filter metrics from a collection
	FilterMetrics(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// ListMetadata - list all metas from a collection
	ListMetadata(collection, tsType string, includeMeta *Metadata, from, maxResults int) ([]Metadata, int, gobol.Error)

	// AddDocuments - add/update a document or a series of documents
	AddDocuments(collection string, metadatas []Metadata) error

	// ListIndexes - list all indexes
	ListIndexes() ([]string, error)

	// CheckMetadata - verifies if a metadata exists
	CheckMetadata(collection, tsType, tsid string) (bool, error)

	// Query - executes a raw query
	Query(collection, query string, from, maxResults int) ([]Metadata, int, gobol.Error)

	// SetRegexValue - add slashes to the value
	SetRegexValue(value string) string
}

// Storage is a storage for metadata
type Storage struct {
	logger *zap.Logger

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// Settings for the metadata package
type Settings struct {
	NumShards         int
	ReplicationFactor int
	URL               string
}

// Metadata document
type Metadata struct {
	ID       string   `json:"id"`
	Metric   string   `json:"metric"`
	TagKey   []string `json:"tagKey"`
	TagValue []string `json:"tagValue"`
	MetaType string   `json:"type"`
}

// Create creates a metadata handler
func Create(settings *Settings, logger *zap.Logger, stats *tsstats.StatsTS) (*Storage, error) {

	backend, err := NewSolrBackend(settings, stats, logger)
	if err != nil {
		return nil, err
	}

	return &Storage{
		logger:  logger,
		Backend: backend,
	}, nil
}
