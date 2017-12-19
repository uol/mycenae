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
		Storage: persist,
	}
}

// Keyspace is a structure that represents the functionality of this module
type Keyspace struct {
	*storage.Storage

	persist *persistence
}

func (keyspace Keyspace) listAllKeyspaces() ([]Config, int, gobol.Error) {
	ks, err := keyspace.persist.listAllKeyspaces()
	return ks, len(ks), err
}

func (keyspace Keyspace) checkKeyspace(key string) gobol.Error {
	return keyspace.persist.checkKeyspace(key)
}

func generateKey() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}

func (keyspace Keyspace) createIndex(esIndex string) gobol.Error {
	return keyspace.persist.createIndex(esIndex)
}

func (keyspace Keyspace) deleteIndex(esIndex string) gobol.Error {
	return keyspace.persist.deleteIndex(esIndex)
}

// GetKeyspace retrieves keyspace metadata
func (keyspace Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}

func (keyspace Keyspace) listDatacenters() ([]string, gobol.Error) {
	return keyspace.persist.listDatacenters()
}
