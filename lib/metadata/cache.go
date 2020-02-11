package metadata

import (
	"encoding/hex"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/uol/hashing"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/memcached"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Manages the the metadata cache
// @author rnojiri

const (
	idNamespace     string = "tsid"
	facetsNamespace string = "fac"
	keysetMapID     string = "map"
	cEmptyArray     string = "[]"
)

// isIDCached - checks if a document id is cached
func (sb *SolrBackend) isIDCached(collection, tsType, tsid string) (bool, error) {

	r, err := sb.memcached.Get([]byte(tsid), idNamespace, collection, tsType, tsid)
	if err != nil {
		return false, err
	}

	return len(r) > 0, nil
}

// cacheID - caches an ID
func (sb *SolrBackend) cacheID(collection, tsType, tsid string) error {

	if sb.idCacheTTL < 0 {
		return nil
	}

	err := sb.memcached.Put([]byte(tsid), tsid, uint16(sb.idCacheTTL), idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// deleteID - remove cached id
func (sb *SolrBackend) deleteCachedID(collection, tsType, tsid string) error {

	err := sb.memcached.Delete([]byte(tsid), idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// getCachedKeysets - return the keysets
func (sb *SolrBackend) getCachedKeysets() ([]string, error) {

	data, gerr := sb.memcached.Get(memcached.ClusterRouter, constants.StringsKSID, keysetMapID)
	if gerr != nil {
		return nil, gerr
	}

	if len(data) > 0 {
		return strings.Split(string(data), constants.StringsWhitespace), nil
	}

	return nil, nil
}

// cacheKeysets - caches the keyset map
func (sb *SolrBackend) cacheKeysets(keysets []string) error {

	if keysets == nil || len(keysets) == 0 {
		return nil
	}

	if sb.keysetCacheTTL < 0 {
		return nil
	}

	value := strings.Trim(fmt.Sprint(keysets), cEmptyArray)
	gerr := sb.memcached.Put(memcached.ClusterRouter, value, uint16(sb.keysetCacheTTL), constants.StringsKSID, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
}

// deleteKeysetMap - deletes the cached keyset map
func (sb *SolrBackend) deleteCachedKeysets() error {

	gerr := sb.memcached.Delete(memcached.ClusterRouter, constants.StringsKSID, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
}

// hash - creates a new hash from a given string
func (sb *SolrBackend) hash(parameters ...interface{}) ([]byte, error) {
	hash, err := hashing.GenerateSHAKE128(sb.cacheKeyHashSize, parameters...)
	if err != nil {
		return nil, errInternalServer("hash", err)
	}
	return hash, nil
}

// getCachedFacets - return all cached facets from the query
func (sb *SolrBackend) getCachedFacets(collection, query string) ([]string, error) {

	hash, gerr := sb.hash(collection, query)
	if gerr != nil {
		return nil, gerr
	}

	data, gerr := sb.memcached.Get(hash, facetsNamespace, collection, hex.EncodeToString(hash))
	if gerr != nil {
		return nil, gerr
	}

	if len(data) > 0 {
		return strings.Split(string(data), constants.StringsWhitespace), nil
	}

	return nil, nil
}

// cacheFacets - caches the facets
func (sb *SolrBackend) cacheFacets(facets []string, collection, query string) error {

	if sb.queryCacheTTL < 0 {
		return nil
	}

	if facets == nil || len(facets) == 0 {
		return nil
	}

	hash, gerr := sb.hash(collection, query)
	if gerr != nil {
		return gerr
	}

	value := strings.Trim(fmt.Sprint(facets), cEmptyArray)
	gerr = sb.memcached.Put(hash, value, uint16(sb.queryCacheTTL), facetsNamespace, collection, hex.EncodeToString(hash))
	if gerr != nil {
		return gerr
	}

	return nil
}
