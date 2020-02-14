package metadata

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/uol/gobol/logh"
	"github.com/uol/hashing"
	"github.com/uol/mycenae/lib/constants"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Manages the the metadata cache
// @author rnojiri

const (
	cEmptyArray string = "[]"
)

var (
	idNamespace     []byte = []byte("tsid")
	facetsNamespace []byte = []byte("fac")
	tsidOK          []byte = []byte("1")
)

// isIDCached - checks if a document id is cached
func (sb *SolrBackend) isIDCached(collection, tsType string, tsid string, tsidBytes []byte) (bool, error) {

	_, exists, err := sb.memcached.Get(tsidBytes, idNamespace, collection, tsType, tsid)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// cacheID - caches an ID
func (sb *SolrBackend) cacheID(collection, tsType, tsid string, tsidBytes []byte) error {

	if sb.noIDCache {
		return nil
	}

	err := sb.memcached.Put(tsidBytes, tsidOK, sb.idCacheTTL, idNamespace, collection, tsType, tsid)
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

// autoUpdateCachedKeysets - automatic updates the cached keysets
func (sb *SolrBackend) autoUpdateCachedKeysets() {

	go func() {
		for {
			<-time.After(sb.keysetCacheAutoUpdateInterval)

			if logh.InfoEnabled {
				sb.logger.Info().Msg("running cached keysets auto update")
			}

			sb.cacheKeysets()
		}
	}()
}

// getCachedKeysets - return the keysets
func (sb *SolrBackend) getCachedKeysets() []string {

	return sb.cachedKeysets
}

// cacheKeysets - caches the keyset map
func (sb *SolrBackend) cacheKeysets() {

	if logh.InfoEnabled {
		sb.logger.Info().Msg("updating cached keysets")
	}

	keysets, err := sb.solrService.ListCollections()
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Send()
		}
	}

	filteredKeysets := []string{}
	for i := 0; i < len(keysets); i++ {
		if _, ok := sb.blacklistedKeysetMap[keysets[i]]; !ok {
			filteredKeysets = append(filteredKeysets, keysets[i])
		}
	}

	sb.cachedKeysets = filteredKeysets
}

// deleteKeysetMap - deletes the cached keyset map
func (sb *SolrBackend) deleteCachedKeysets() {

	sb.cacheKeysets()
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

	data, exists, gerr := sb.memcached.Get(hash, facetsNamespace, collection, hex.EncodeToString(hash))
	if gerr != nil {
		return nil, gerr
	}

	if exists {
		return strings.Split(string(data), constants.StringsWhitespace), nil
	}

	return nil, nil
}

// cacheFacets - caches the facets
func (sb *SolrBackend) cacheFacets(facets []string, collection, query string) error {

	if sb.noQueryCache {
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
	gerr = sb.memcached.Put(hash, []byte(value), sb.queryCacheTTL, facetsNamespace, collection, hex.EncodeToString(hash))
	if gerr != nil {
		return gerr
	}

	return nil
}
