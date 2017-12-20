package keyspace

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/tsstats"
)

var (
	maxTTL   int
	validKey *regexp.Regexp
	stats    *tsstats.StatsTS
)

func New(
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	es *rubber.Elastic,
	usernameGrant,
	keyspaceMain string,
	mTTL int,
) *Keyspace {

	maxTTL = mTTL
	validKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)
	stats = sts

	return &Keyspace{
		persist: persistence{
			cassandra:     cass,
			esearch:       es,
			usernameGrant: usernameGrant,
			keyspaceMain:  keyspaceMain,
		},
	}
}

type Keyspace struct {
	persist persistence
}

func (keyspace Keyspace) createKeyspace(ksc Config) (string, gobol.Error) {

	count, gerr := keyspace.persist.countKeyspaceByName(ksc.Name)
	if gerr != nil {
		return "", gerr
	}
	if count != 0 {
		return "", errConflict(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because keyspace "%s" already exists`, ksc.Name),
		)
	}

	count, gerr = keyspace.persist.countDatacenterByName(ksc.Datacenter)
	if gerr != nil {
		return "", gerr
	}
	if count == 0 {
		return "", errValidationS(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because datacenter "%s" not exists`, ksc.Datacenter),
		)
	}

	key := generateKey()

	gerr = keyspace.persist.createKeyspace(ksc, key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	gerr = keyspace.createIndex(key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		gerr2 = keyspace.deleteIndex(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	gerr = keyspace.persist.createKeyspaceMeta(ksc, key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		gerr2 = keyspace.deleteIndex(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	return key, nil
}

func (keyspace Keyspace) updateKeyspace(ksc ConfigUpdate, key string) gobol.Error {

	count, gerr := keyspace.persist.countKeyspaceByKey(key)
	if gerr != nil {
		return gerr
	}
	if count == 0 {
		return errNotFound("UpdateKeyspace")

	}

	count, gerr = keyspace.persist.countKeyspaceByName(ksc.Name)
	if gerr != nil {
		return gerr
	}
	if count != 0 {
		k, gerr := keyspace.persist.getKeyspaceKeyByName(ksc.Name)
		if gerr != nil {
			return gerr
		}

		if k != key {
			return errConflict(
				"UpdateKeyspace",
				fmt.Sprintf(`Cannot update because keyspace "%s" already exists`, ksc.Name),
			)
		}
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

func generateKey() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}

func (keyspace Keyspace) createIndex(esIndex string) gobol.Error {
	return keyspace.persist.createIndex(esIndex)
}

func (keyspace Keyspace) deleteIndex(esIndex string) gobol.Error {
	return keyspace.persist.deleteIndex(esIndex)
}

func (keyspace Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}

func (keyspace Keyspace) listDatacenters() ([]string, gobol.Error) {
	return keyspace.persist.listDatacenters()
}
