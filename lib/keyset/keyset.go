package keyset

import (
	"net/http"
	"regexp"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tsstats"
)

// Manages all keyset CRUD and offers some API
// @author: rnojiri

// KeySet - the keyset
type KeySet struct {
	storage      *metadata.Storage
	stats        *tsstats.StatsTS
	keySetRegexp *regexp.Regexp
}

// NewKeySet - initializes
func NewKeySet(storage *metadata.Storage, s *tsstats.StatsTS) *KeySet {
	return &KeySet{
		storage:      storage,
		stats:        s,
		keySetRegexp: regexp.MustCompile(`^[a-z_]{1}[a-z0-9_\-]+[a-z0-9]{1}$`),
	}
}

// IsKeySetNameValid - checks if the keyset name is valid
func (ks *KeySet) IsKeySetNameValid(keySet string) bool {
	return ks.keySetRegexp.MatchString(keySet)
}

// CreateIndex - creates a new index
func (ks *KeySet) CreateIndex(esIndex string) gobol.Error {

	if !ks.IsKeySetNameValid(esIndex) {
		return errBadRequest("CreateIndex", "invalid keyset name format")
	}

	start := time.Now()

	err := ks.storage.CreateKeySet(esIndex)
	if err != nil {
		ks.statsIndexError(esIndex, "CreateIndex")
		return errInternalServerError("CreateIndex", err)
	}

	ks.statsIndex(esIndex, "CreateIndex", time.Since(start))
	return nil
}

// deleteIndex - deletes the index
func (ks *KeySet) deleteIndex(esIndex string) gobol.Error {

	start := time.Now()

	err := ks.storage.DeleteKeySet(esIndex)
	if err != nil {
		ks.statsIndexError(esIndex, "deleteIndex")
		return errInternalServerError("deleteIndex", err)
	}

	ks.statsIndex(esIndex, "deleteIndex", time.Since(start))
	return nil
}

// Check if a keyspace exists
func (ks *KeySet) Check(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keyset := ps.ByName("keyset")
	if keyset == constants.StringsEmpty {
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

	found, err := ks.storage.CheckKeySet(keyset)
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
