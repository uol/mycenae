package keyset

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/tserr"
	"github.com/uol/mycenae/lib/tsstats"
)

type KeySet struct {
	elastic      *rubber.Elastic
	stats        *tsstats.StatsTS
	keySetRegexp *regexp.Regexp
	memcached    *memcached.Memcached
}

func NewKeySet(e *rubber.Elastic, s *tsstats.StatsTS, memcached *memcached.Memcached) *KeySet {
	return &KeySet{
		elastic:      e,
		stats:        s,
		keySetRegexp: regexp.MustCompile(`^[A-Za-z_]{1}[A-Za-z0-9_]+$`),
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

	body := &bytes.Buffer{}

	body.WriteString(
		`{"mappings":{"meta":{"properties":{"tagsNested":{"type":"nested","properties":{"tagKey":{"type":"string"},"tagValue":{"type":"string"}}}}},"metatext":{"properties":{"tagsNested":{"type":"nested","properties":{"tagKey":{"type":"string"},"tagValue":{"type":"string"}}}}}}}`,
	)

	_, err := ks.elastic.CreateIndex(esIndex, body)
	if err != nil {
		ks.statsIndexError(esIndex, "", "post")
		return errInternalServerError("CreateIndex", err)
	}

	gerr := ks.memcached.Delete("keyset", "map")

	if gerr != nil {
		ks.statsIndexError(esIndex, "", "post")
		return errInternalServerError("CreateIndex", err)
	}

	ks.statsIndex(esIndex, "", "post", time.Since(start))
	return nil
}

func (ks *KeySet) deleteIndex(esIndex string) gobol.Error {

	start := time.Now()

	_, err := ks.elastic.DeleteIndex(esIndex)
	if err != nil {
		ks.statsIndexError(esIndex, "", "delete")
		return errInternalServerError("deleteIndex", err)
	}

	gerr := ks.memcached.Delete("keyset", "map")

	if gerr != nil {
		ks.statsIndexError(esIndex, "", "delete")
		return errInternalServerError("deleteIndex", err)
	}

	ks.statsIndex(esIndex, "", "delete", time.Since(start))
	return nil
}

func (ks *KeySet) getAllIndexes() ([]string, gobol.Error) {

	start := time.Now()

	status, response, err := ks.elastic.Request("_stats", "GET", "index", nil)
	if err != nil {
		ks.statsIndexError("_stats", "", "get")
		return nil, errInternalServerError("getAllIndexes", err)
	}

	if status != http.StatusOK {
		ks.statsIndexError("_stats", "", "get")
		return nil, errInternalServerError("getAllIndexes", err)
	}

	jsonData := map[string]map[string]interface{}{}
	err = json.Unmarshal(response, &jsonData)
	if err != nil {
		return nil, errInternalServerError("getAllIndexes", err)
	}

	var results []string
	for k, _ := range jsonData["indices"] {
		results = append(results, k)
	}

	ks.statsIndex("_stats", "", "get", time.Since(start))

	return results, nil
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
