package metadata

import (
	"time"

	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/uol/gobol"
)

//
// Manages the the metadata index operations
// @author rnojiri
//

// CreateKeyset - creates a new collection
func (sb *SolrBackend) CreateKeyset(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.CreateCollection(collection, sb.zookeeperConfig, sb.numShards, sb.replicationFactor)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "CreateKeyset").Msg("error on creating collection")
		}
		sb.statsCollectionError(collection, "create", "solr.collection.adm.error")
		return errInternalServer("CreateKeyset", err)
	}

	err = sb.deleteCachedKeysets()
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "CreateKeyset").Msg("error deleting cached keyset map")
		}
		sb.statsCollectionError(collection, "create", "solr.collection.adm.error")
		return errInternalServer("CreateKeyset", err)
	}

	sb.statsCollectionAction(collection, "create", "solr.collection.adm", time.Since(start))
	return nil
}

// DeleteKeyset - deletes a collection
func (sb *SolrBackend) DeleteKeyset(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.DeleteCollection(collection)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "DeleteKeyset").Msg("error deleting collection")
		}
		sb.statsCollectionError(collection, "delete", "solr.collection.adm.error")
		return errInternalServer("DeleteKeyset", err)
	}

	err = sb.deleteCachedKeysets()
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "DeleteKeyset").Msg("error deleting cached keyset map")
		}
		sb.statsCollectionError(collection, "delete", "solr.collection.adm.error")
		return errInternalServer("DeleteKeyset", err)
	}

	sb.statsCollectionAction(collection, "delete", "solr.collection.adm", time.Since(start))
	return nil
}

// ListKeysets - list all keysets
func (sb *SolrBackend) ListKeysets() ([]string, gobol.Error) {

	start := time.Now()

	cachedKeysets, err := sb.getCachedKeysets()
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return nil, errInternalServer("ListKeysets", err)
	}

	if cachedKeysets != nil && len(cachedKeysets) > 0 {
		sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))
		return cachedKeysets, nil
	}

	keysets, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return nil, errInternalServer("ListKeysets", e)
	}

	filteredKeysets := []string{}
	for i := 0; i < len(keysets); i++ {
		if _, ok := sb.blacklistedKeysetMap[keysets[i]]; !ok {
			filteredKeysets = append(filteredKeysets, keysets[i])
		}
	}

	err = sb.cacheKeysets(filteredKeysets)
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return nil, errInternalServer("ListKeysets", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))
	return filteredKeysets, nil
}

// CheckKeyset - verifies if an index exists
func (sb *SolrBackend) CheckKeyset(keyset string) (bool, gobol.Error) {

	keysets, err := sb.getCachedKeysets()
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return false, errInternalServer("CheckKeyset", err)
	}

	if len(keysets) > 0 {
		for _, k := range keysets {
			if k == keyset {
				return true, nil
			}
		}

		return false, nil
	}

	start := time.Now()

	keysets, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return false, errInternalServer("CheckKeyset", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))

	filteredKeysets := []string{}
	for i := 0; i < len(keysets); i++ {
		if _, ok := sb.blacklistedKeysetMap[keysets[i]]; !ok {
			filteredKeysets = append(filteredKeysets, keysets[i])
		}
	}

	err = sb.cacheKeysets(filteredKeysets)
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return false, errInternalServer("CheckKeyset", e)
	}

	for _, k := range keysets {
		if k == keyset {
			return true, nil
		}
	}

	return false, nil
}
