package keyspace

import (
	"regexp"
	"strings"

	"github.com/pborman/uuid"
	"github.com/uol/gobol"

	storage "github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
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
	devMode bool,
	defaultTTL uint8,
) *Keyspace {

	validKey = regexp.MustCompile(`^[A-Za-z]{1}[0-9A-Za-z_]+$`)
	stats = sts

	if compaction == "" {
		compaction = DefaultCompaction
	}

	return &Keyspace{
		Storage: persist,
		devMode:    devMode,
		defaultTTL: defaultTTL,
	}
}

// Keyspace is a structure that represents the functionality of this module
type Keyspace struct {
	*storage.Storage
	persist *persistence
}

// GetKeyspace retrieves keyspace metadata
func (keyspace Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}