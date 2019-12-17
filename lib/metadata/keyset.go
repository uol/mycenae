package metadata

import (
	"time"

	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/uol/gobol"
)

//
// Manages the the metadata index operations
// @author rnojiri
//

// CreateKeySet - creates a new collection
func (sb *SolrBackend) CreateKeySet(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.CreateCollection(collection, sb.zookeeperConfig, sb.numShards, sb.replicationFactor)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "CreateKeySet").Msg("error on creating collection")
		}
		sb.statsCollectionError(collection, "create", "solr.collection.adm.error")
		return errInternalServer("CreateKeySet", err)
	}

	err = sb.deleteCachedKeySets()
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "CreateKeySet").Msg("error deleting cached keyset map")
		}
		sb.statsCollectionError(collection, "create", "solr.collection.adm.error")
		return errInternalServer("CreateKeySet", err)
	}

	sb.statsCollectionAction(collection, "create", "solr.collection.adm", time.Since(start))
	return nil
}

// DeleteKeySet - deletes a collection
func (sb *SolrBackend) DeleteKeySet(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.DeleteCollection(collection)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "DeleteKeySet").Msg("error deleting collection")
		}
		sb.statsCollectionError(collection, "delete", "solr.collection.adm.error")
		return errInternalServer("DeleteKeySet", err)
	}

	err = sb.deleteCachedKeySets()
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "DeleteKeySet").Msg("error deleting cached keyset map")
		}
		sb.statsCollectionError(collection, "delete", "solr.collection.adm.error")
		return errInternalServer("DeleteKeySet", err)
	}

	sb.statsCollectionAction(collection, "delete", "solr.collection.adm", time.Since(start))
	return nil
}

// ListKeySets - list all keysets
func (sb *SolrBackend) ListKeySets() ([]string, gobol.Error) {

	start := time.Now()

	cachedKeysets, err := sb.getCachedKeysets()
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return nil, errInternalServer("ListKeySets", err)
	}

	if cachedKeysets != nil && len(cachedKeysets) > 0 {
		sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))
		return cachedKeysets, nil
	}

	keysets, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return nil, errInternalServer("ListKeySets", e)
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
		return nil, errInternalServer("ListKeySets", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))
	return filteredKeysets, nil
}

// CheckKeySet - verifies if an index exists
func (sb *SolrBackend) CheckKeySet(keyset string) (bool, gobol.Error) {

	start := time.Now()

	keysets, err := sb.getCachedKeysets()
	if err != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return false, errInternalServer("CheckKeySet", err)
	}

	if len(keysets) > 0 {
		for _, k := range keysets {
			if k == keyset {
				return true, nil
			}
		}

		return false, nil
	}

	keysets, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.adm.error")
		return false, errInternalServer("CheckKeySet", e)
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
		return false, errInternalServer("CheckKeySet", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.adm", time.Since(start))
	for _, k := range keysets {
		if k == keyset {
			return true, nil
		}
	}

	return false, nil
}
