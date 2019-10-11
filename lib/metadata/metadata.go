package metadata

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

// Backend hides the underlying implementation of the metadata storage
type Backend interface {
	// CreateKeySet creates a keyset in the metadata storage
	CreateKeySet(name string) gobol.Error

	// DeleteKeySet deletes a keyset in the metadata storage
	DeleteKeySet(name string) gobol.Error

	// ListKeySet - list all keyset
	ListKeySets() ([]string, gobol.Error)

	// CheckKeySet - verifies if a keyset exists
	CheckKeySet(keyset string) (bool, gobol.Error)

	// FilterTagValues - filter tag values from a collection
	FilterTagValues(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterTagKeys - filter tag keys from a collection
	FilterTagKeys(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterMetrics - filter metrics from a collection
	FilterMetrics(collection, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterMetadata - list all metas from a collection
	// Returns: results, total and gobol.Error
	FilterMetadata(collection string, query *Query, from, maxResults int) ([]Metadata, int, gobol.Error)

	// AddDocument - add/update a document
	AddDocument(collection string, metadata *Metadata) gobol.Error

	// CheckMetadata - verifies if a metadata exists
	CheckMetadata(collection, tsType, tsid string) (bool, gobol.Error)

	// SetRegexValue - add slashes to the value
	SetRegexValue(value string) string

	// HasRegexPattern - check if the value has a regular expression
	HasRegexPattern(value string) bool

	// DeleteDocumentByID - delete a document by ID and its child documents
	DeleteDocumentByID(collection, tsType, id string) gobol.Error

	// FilterTagKeysByMetric - filter tag values from a collection given its metric
	FilterTagKeysByMetric(collection, tsType, metric, prefix string, maxResults int) ([]string, int, gobol.Error)

	// FilterTagValuesByMetricAndTag - filter tag values from a collection given its metric and tag
	FilterTagValuesByMetricAndTag(collection, tsType, metric, tag, prefix string, maxResults int) ([]string, int, gobol.Error)
}

// Storage is a storage for metadata
type Storage struct {
	logger *zap.Logger

	// Backend is the thing that actually does the specific work in the storage
	Backend
}

// Settings for the metadata package
type Settings struct {
	NumShards           int
	ReplicationFactor   int
	URL                 string
	IDCacheTTL          int32
	QueryCacheTTL       int32
	KeysetCacheTTL      int32
	MaxReturnedMetadata int
	ZookeeperConfig     string
	BlacklistedKeysets  []string
}

// Metadata document
type Metadata struct {
	ID       string   `json:"id"`
	Metric   string   `json:"metric"`
	TagKey   []string `json:"tagKey"`
	TagValue []string `json:"tagValue"`
	MetaType string   `json:"type"`
	Keyset   string   `json:"keyset"`
}

// Query - query
type Query struct {
	Metric   string     `json:"metric"`
	MetaType string     `json:"type"`
	Regexp   bool       `json:regexp`
	Tags     []QueryTag `json:"tags"`
}

// QueryTag - tags for query
type QueryTag struct {
	Key    string   `json:"key"`
	Values []string `json:value`
	Negate bool     `json:negate`
	Regexp bool     `json:regexp`
}

// Create creates a metadata handler
func Create(settings *Settings, logger *zap.Logger, stats *tsstats.StatsTS, memcached *memcached.Memcached) (*Storage, error) {

	backend, err := NewSolrBackend(settings, stats, logger, memcached)
	if err != nil {
		return nil, err
	}

	return &Storage{
		logger:  logger,
		Backend: backend,
	}, nil
}
