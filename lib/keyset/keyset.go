package keyset

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tserr"
	"github.com/uol/mycenae/lib/tsstats"
)

// KeySet - the keyset
type KeySet struct {
	storage      *metadata.Storage
	stats        *tsstats.StatsTS
	keySetRegexp *regexp.Regexp
	memcached    *memcached.Memcached
}

func NewKeySet(storage *metadata.Storage, s *tsstats.StatsTS, memcached *memcached.Memcached) *KeySet {
	return &KeySet{
		storage:      storage,
		stats:        s,
		keySetRegexp: regexp.MustCompile(`^[a-z_]{1}[a-z0-9_]+$`),
		memcached:    memcached,
	}
}

func (ks *KeySet) IsKeySetNameValid(keySet string) bool {
	return ks.keySetRegexp.MatchString(keySet)
}

func (ks *KeySet) CreateIndex(esIndex string) gobol.Error {

	if !ks.IsKeySetNameValid(esIndex) {
		return errBadRequest("CreateIndex", "invalid keyset name format")
	}

	start := time.Now()

	err := ks.storage.CreateIndex(esIndex)
	if err != nil {
		ks.statsIndexError(esIndex, "CreateIndex")
		return errInternalServerError("CreateIndex", err)
	}

	gerr := ks.memcached.Delete("keyset", "map")

	if gerr != nil {
		ks.statsIndexError(esIndex, "CreateIndex")
		return errInternalServerError("CreateIndex", err)
	}

	ks.statsIndex(esIndex, "CreateIndex", time.Since(start))
	return nil
}

func (ks *KeySet) deleteIndex(esIndex string) gobol.Error {

	start := time.Now()

	err := ks.storage.DeleteIndex(esIndex)
	if err != nil {
		ks.statsIndexError(esIndex, "deleteIndex")
		return errInternalServerError("deleteIndex", err)
	}

	gerr := ks.memcached.Delete("keyset", "map")

	if gerr != nil {
		ks.statsIndexError(esIndex, "deleteIndex")
		return errInternalServerError("deleteIndex", err)
	}

	ks.statsIndex(esIndex, "deleteIndex", time.Since(start))
	return nil
}

func (ks *KeySet) getAllIndexes() ([]string, gobol.Error) {

	start := time.Now()

	indexes, err := ks.storage.ListIndexes()
	if err != nil {
		ks.statsIndexError("all", "getAllIndexes")
		return nil, errInternalServerError("getAllIndexes", err)
	}

	ks.statsIndex("all", "getAllIndexes", time.Since(start))

	return indexes, nil
}

func (ks *KeySet) getKeySetMap() (map[string]bool, gobol.Error) {

	m, gerr := ks.memcached.Get("keyset", "map")
	if gerr != nil {
		return nil, gerr
	}

	if m != nil {
		keySetMap := map[string]bool{}
		err := json.Unmarshal(m, &keySetMap)
		if err != nil {
			return nil, tserr.New(err, "Error converting binary to map.", http.StatusInternalServerError, nil)
		}

		return keySetMap, nil
	}

	indexes, gerr := ks.getAllIndexes()
	if gerr != nil {
		if gerr.StatusCode() == http.StatusNotFound {
			return nil, nil
		}
		return nil, gerr
	}

	if indexes == nil || len(indexes) == 0 {
		return nil, nil
	}

	keySetMap := map[string]bool{}
	for _, v := range indexes {
		keySetMap[v] = true
	}

	data, err := json.Marshal(keySetMap)
	if err != nil {
		return nil, tserr.New(err, "Error converting map to binary.", http.StatusInternalServerError, nil)
	}

	gerr = ks.memcached.Put("keyset", "map", data)
	if gerr != nil {
		return nil, gerr
	}

	return keySetMap, nil
}

func (ks *KeySet) KeySetExists(key string) (bool, gobol.Error) {

	keySetMap, gerr := ks.getKeySetMap()

	if gerr != nil {
		return false, gerr
	}

	if keySetMap == nil {
		return false, nil
	}

	return keySetMap[key], nil
}

// Check if a keyspace exists
func (ks *KeySet) Check(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path":   "/keysets/#keyset",
				"keyset": "empty",
			},
		)
		rip.Fail(w, errNotFound("Check"))
		return
	}

	found, err := ks.KeySetExists(keyset)
	if err != nil {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path": "/keysets/#keyset",
			},
		)
		rip.Fail(w, err)
		return
	}

	if !found {
		rip.Fail(w, errNotFound(
			"Check",
		))
		return
	}

	rip.AddStatsMap(
		r,
		map[string]string{
			"path":   "/keysets/#keyset",
			"keyset": keyset,
		},
	)
	rip.Success(w, http.StatusOK, nil)
}
