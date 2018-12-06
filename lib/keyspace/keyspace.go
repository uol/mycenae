package keyspace

import (
	"regexp"

	"github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/tsstats"
)

var validKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)

// New creates a new keyspace manager
func New(
	sts *tsstats.StatsTS,
	storage *persistence.Storage,
	devMode bool,
	defaultTTL int,
	maxAllowedTTL int,
) *Keyspace {
	return &Keyspace{
		Storage:       storage,
		stats:         sts,
		devMode:       devMode,
		defaultTTL:    defaultTTL,
		maxAllowedTTL: maxAllowedTTL,
	}
}

// Keyspace is a structure that represents the functionality of this module
type Keyspace struct {
	*persistence.Storage
	stats         *tsstats.StatsTS
	devMode       bool
	defaultTTL    int
	maxAllowedTTL int
}
