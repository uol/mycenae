package metadata

import (
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/hashstructure"
	"github.com/uol/gobol"
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
func (sb *SolrBackend) deleteCachedID(collection, tsType, tsid string) gobol.Error {

	err := sb.memcached.Delete(idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// getCachedKeysets - return the keysets
func (sb *SolrBackend) getCachedKeysets() ([]string, gobol.Error) {

	data, gerr := sb.memcached.Get(keysetNamespace, keysetMapID)
	if gerr != nil {
		return nil, gerr
	}

	if len(data) > 0 {
		return strings.Split(string(data), " "), nil
	}

	return nil, nil
}

// cacheKeysets - caches the keyset map
func (sb *SolrBackend) cacheKeysets(keysets []string) gobol.Error {

	if keysets == nil || len(keysets) == 0 {
		return nil
	}

	if sb.keysetCacheTTL < 0 {
		return nil
	}

	gerr := sb.memcached.Put([]byte(strings.Trim(fmt.Sprint(keysets), "[]")), sb.keysetCacheTTL, keysetNamespace, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
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
func (sb *SolrBackend) hash(v interface{}) (string, gobol.Error) {
	hash, err := hashstructure.Hash(v, nil)
	if err != nil {
		return "", errInternalServer("hash", err)
	}
	return strconv.FormatUint(hash, 10), nil
}

// getCachedFacets - return all cached facets from the query
func (sb *SolrBackend) getCachedFacets(collection, field string, v interface{}) ([]string, gobol.Error) {

	hash, gerr := sb.hash(v)
	if gerr != nil {
		return nil, gerr
	}

	data, gerr := sb.memcached.Get(facetsNamespace, collection, field, hash)
	if gerr != nil {
		return nil, gerr
	}

	if len(data) > 0 {
		return strings.Split(string(data), " "), nil
	}

	return nil, nil
}

// cacheFacets - caches the facets
func (sb *SolrBackend) cacheFacets(facets []string, collection, field string, v interface{}) gobol.Error {

	if sb.queryCacheTTL < 0 {
		return nil
	}

	if facets == nil || len(facets) == 0 {
		return nil
	}

	hash, gerr := sb.hash(v)
	if gerr != nil {
		return gerr
	}

	gerr = sb.memcached.Put([]byte(strings.Trim(fmt.Sprint(facets), "[]")), sb.queryCacheTTL, facetsNamespace, collection, field, hash)
	if gerr != nil {
		return gerr
	}

	return nil
}
