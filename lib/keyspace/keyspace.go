package keyspace

import (
	"regexp"

	"github.com/uol/gobol"

	storage "github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	maxTTL   int
	validKey *regexp.Regexp
	stats    *tsstats.StatsTS
)

// DefaultCompaction defines the default compaction strategy that cassandra
// will use for timeseries data
const DefaultCompaction = "com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy"

// New creates a new keyspace manager
func New(
	sts *tsstats.StatsTS,
	persist *storage.Storage,
	usernameGrant,
	keyspaceMain string,
	compaction string,
	mTTL int,
) *Keyspace {

	maxTTL = mTTL
	validKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)
	stats = sts

	if compaction == "" {
		compaction = DefaultCompaction
	}

	return &Keyspace{
		storage: persist,
	}
}

// Keyspace is a structure that represents the functionality of this module
type Keyspace struct {
	storage *storage.Storage
	persist *persistence
}

// GetKeyspace retrieves keyspace metadata
func (keyspace Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}
