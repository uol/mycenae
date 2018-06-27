package metadata

import (
	"net/http"
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/hashstructure"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Manages the the metadata cache
// @author rnojiri

const idNamespace = "tsid"
const keysetNamespace = "ksid"
const facetsNamespace = "fac"
const keysetMapID = "map"

// isIDCached - checks if a document id is cached
func (sb *SolrBackend) isIDCached(collection, tsType, tsid string) (bool, gobol.Error) {

	r, err := sb.memcached.Get(idNamespace, collection, tsType, tsid)
	if err != nil {
		return false, err
	}

	return len(r) > 0, nil
}

// cacheID - caches an ID
func (sb *SolrBackend) cacheID(collection, tsType, tsid string) gobol.Error {

	if sb.idCacheTTL < 0 {
        return nil
	}

	err := sb.memcached.Put([]byte(tsid), sb.idCacheTTL, idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// deleteID - remove cached id
func (sb *SolrBackend) deleteCacheID(collection, tsType, tsid string) gobol.Error {

	err := sb.memcached.Delete(idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// getKeySetMap - return the keyset map
func (sb *SolrBackend) getCachedKeySetMap() (map[string]bool, gobol.Error) {

	m, gerr := sb.memcached.Get(keysetNamespace, keysetMapID)
	if gerr != nil {
		return nil, gerr
	}

	if m != nil {
		keySetMap := map[string]bool{}
		err := json.Unmarshal(m, &keySetMap)
		if err != nil {
			return nil, tserr.New(err, "error converting binary to map", http.StatusInternalServerError, nil)
		}

		return keySetMap, nil
	}

	return nil, nil
}

// cacheKeysetMap - caches the keyset map
func (sb *SolrBackend) cacheKeySetMap(keysets []string) (map[string]bool, gobol.Error) {

	if sb.keysetCacheTTL < 0 {
        return nil, nil
	}

	if keysets == nil || len(keysets) == 0 {
		return nil, nil
	}

	keysetMap := map[string]bool{}
	for _, v := range keysets {
		keysetMap[v] = true
	}

	data, err := json.Marshal(keysetMap)
	if err != nil {
		return nil, tserr.New(err, "error converting map to binary", http.StatusInternalServerError, nil)
	}

	gerr := sb.memcached.Put(data, sb.keysetCacheTTL, keysetNamespace, keysetMapID)
	if gerr != nil {
		return nil, gerr
	}

	return keysetMap, nil
}

// deleteKeySetMap - deletes the cached keyset map
func (sb *SolrBackend) deleteCachedKeySetMap() gobol.Error {

	gerr := sb.memcached.Delete(keysetNamespace, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
}

// hash - creates a new hash from a given string
func (sb *SolrBackend) hash(query *Query) (string, gobol.Error) {
	hash, err := hashstructure.Hash(*query, nil)
	if err != nil {
		return "", errInternalServer("hash", err)
	}
	return strconv.FormatUint(hash, 10), nil
}

// getCachedFacets - return all cached facets from the query
func (sb *SolrBackend) getCachedFacets(collection, field string, query *Query) ([]string, gobol.Error) {

	hash, gerr := sb.hash(query)
	if gerr != nil {
		return nil, gerr
	}

	f, gerr := sb.memcached.Get(facetsNamespace, collection, field, hash)
	if gerr != nil {
		return nil, gerr
	}

	if f != nil {
		facets := []string{}
		err := json.Unmarshal(f, &facets)
		if err != nil {
			return nil, tserr.New(err, "error converting binary to string array", http.StatusInternalServerError, nil)
		}

		return facets, nil
	}

	return nil, nil
}

// cacheFacets - caches the facets
func (sb *SolrBackend) cacheFacets(facets []string, collection, field string, query *Query) gobol.Error {

	if sb.queryCacheTTL < 0 {
        return nil
	}

	if facets == nil || len(facets) == 0 {
		return nil
	}

	data, err := json.Marshal(facets)
	if err != nil {
		return tserr.New(err, "error converting string array to binary", http.StatusInternalServerError, nil)
	}

	hash, gerr := sb.hash(query)
	if gerr != nil {
		return gerr
	}

	gerr = sb.memcached.Put(data, sb.queryCacheTTL, facetsNamespace, collection, field, hash)
	if gerr != nil {
		return gerr
	}

	return nil
}
