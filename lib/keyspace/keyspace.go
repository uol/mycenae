package keyspace

import (
	"regexp"

	"github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/tsstats"
)

// DefaultCompaction defines the default compaction strategy that cassandra
// will use for timeseries data
const DefaultCompaction = "com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy"

var validKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)

// New creates a new keyspace manager
func New(
	sts *tsstats.StatsTS,
	storage *persistence.Storage,
	mTTL int,
) *Keyspace {
	return &Keyspace{
		Storage: storage,
		maxTTL:  mTTL,
		stats:   sts,
	}
}

// Keyspace is a structure that represents the functionality of this module
type Keyspace struct {
	*persistence.Storage

	maxTTL   int
	validKey *regexp.Regexp
	stats    *tsstats.StatsTS
}
