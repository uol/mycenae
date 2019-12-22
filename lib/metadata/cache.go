package metadata

import (
	"fmt"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/hashstructure"
	"github.com/uol/mycenae/lib/constants"
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

	r, err := sb.memcached.Get(idNamespace, collection, tsType, tsid)
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

	err := sb.memcached.Put([]byte(tsid), sb.idCacheTTL, idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// deleteID - remove cached id
func (sb *SolrBackend) deleteCachedID(collection, tsType, tsid string) error {

	err := sb.memcached.Delete(idNamespace, collection, tsType, tsid)
	if err != nil {
		return err
	}

	return nil
}

// getCachedKeysets - return the keysets
func (sb *SolrBackend) getCachedKeysets() ([]string, error) {

	data, gerr := sb.memcached.Get(constants.StringsKSID, keysetMapID)
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

	gerr := sb.memcached.Put([]byte(strings.Trim(fmt.Sprint(keysets), cEmptyArray)), sb.keysetCacheTTL, constants.StringsKSID, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
}

// deleteKeysetMap - deletes the cached keyset map
func (sb *SolrBackend) deleteCachedKeysets() error {

	gerr := sb.memcached.Delete(constants.StringsKSID, keysetMapID)
	if gerr != nil {
		return gerr
	}

	return nil
}

// hash - creates a new hash from a given string
func (sb *SolrBackend) hash(v interface{}) (string, error) {
	hash, err := hashstructure.Hash(v, nil)
	if err != nil {
		return constants.StringsEmpty, errInternalServer("hash", err)
	}
	return strconv.FormatUint(hash, 10), nil
}

// getCachedFacets - return all cached facets from the query
func (sb *SolrBackend) getCachedFacets(collection, field string, v interface{}) ([]string, error) {

	hash, gerr := sb.hash(v)
	if gerr != nil {
		return nil, gerr
	}

	data, gerr := sb.memcached.Get(facetsNamespace, collection, field, hash)
	if gerr != nil {
		return nil, gerr
	}

	if len(data) > 0 {
		return strings.Split(string(data), constants.StringsWhitespace), nil
	}

	return nil, nil
}

// cacheFacets - caches the facets
func (sb *SolrBackend) cacheFacets(facets []string, collection, field string, v interface{}) error {

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

	gerr = sb.memcached.Put([]byte(strings.Trim(fmt.Sprint(facets), cEmptyArray)), sb.queryCacheTTL, facetsNamespace, collection, field, hash)
	if gerr != nil {
		return gerr
	}

	return nil
}
