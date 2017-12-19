package keyspace

import (
	"regexp"
	"strings"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/tsstats"
)

var (
	validKey *regexp.Regexp
	stats    *tsstats.StatsTS
)

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
	devMode    bool
	defaultTTL uint8
}

func (keyspace Keyspace) CreateKeyspace(ksc Config) gobol.Error {

	count, gerr := keyspace.persist.countKeyspaceByKey(ksc.Name)
	if gerr != nil {
		return gerr
	}
	if count != 0 {
		return errConflict(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because keyspace "%s" already exists`, ksc.Name),
		)
	}

	count, gerr = keyspace.persist.countDatacenterByName(ksc.Datacenter)
	if gerr != nil {
		return gerr
	}
	if count == 0 {
		return errValidationS(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because datacenter "%s" not exists`, ksc.Datacenter),
		)
	}

	if keyspace.devMode {
		ksc.TTL = keyspace.defaultTTL
	}

	gerr = keyspace.persist.createKeyspace(ksc)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(ksc.Name)
		if gerr2 != nil {

		}
		return gerr
	}

	gerr = keyspace.persist.createKeyspaceMeta(ksc)
	if gerr != nil {
		gerr1 := keyspace.persist.dropKeyspace(ksc.Name)
		if gerr1 != nil {

		}
		return gerr
	}

	return nil
}

func (keyspace Keyspace) updateKeyspace(ksc ConfigUpdate, key string) gobol.Error {

	count, gerr := keyspace.persist.countKeyspaceByKey(key)
	if gerr != nil {
		return gerr
	}
	if count == 0 {
		return errNotFound("UpdateKeyspace")

	}

	return keyspace.persist.updateKeyspace(ksc, key)
}

func (keyspace Keyspace) listAllKeyspaces() ([]Config, int, gobol.Error) {
	ks, err := keyspace.persist.listAllKeyspaces()
	return ks, len(ks), err
}

func (keyspace Keyspace) checkKeyspace(key string) gobol.Error {
	return keyspace.persist.checkKeyspace(key)
}

// GetKeyspace retrieves keyspace metadata
func (keyspace Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}

func (keyspace Keyspace) listDatacenters() ([]string, gobol.Error) {
	return keyspace.persist.listDatacenters()
}
