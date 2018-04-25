package metadata

import (
	"time"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Manages the the metadata index operations
// @author rnojiri

// CreateKeySet - creates a new collection
func (sb *SolrBackend) CreateKeySet(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.CreateCollection(collection, sb.numShards, sb.replicationFactor)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "CreateKeySet"),
			zap.String("step", "CreateCollection"),
		}
		sb.logger.Error("error on creating collection", lf...)
		sb.statsCollectionError(collection, "create", "solr.collection.action")
		return errInternalServer("CreateKeySet", err)
	}

	err = sb.setupSchema(collection)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "CreateKeySet"),
			zap.String("step", "setupSchema"),
		}
		sb.logger.Error("error on schema setup", lf...)
		return errInternalServer("CreateKeySet", err)
	}

	err = sb.deleteCachedKeySetMap()
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "CreateKeySet"),
			zap.String("step", "deleteCachedKeySetMap"),
		}
		sb.logger.Error("error deleting keyset map", lf...)
		return errInternalServer("CreateKeySet", err)
	}

	sb.statsCollectionAction(collection, "create", "solr.collection.action", time.Since(start))
	return nil
}

// DeleteKeySet - deletes a collection
func (sb *SolrBackend) DeleteKeySet(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.DeleteCollection(collection)
	if err != nil {
		sb.statsCollectionError(collection, "delete", "solr.collection.create.error")
		return errInternalServer("DeleteKeySet", err)
	}

	err = sb.deleteCachedKeySetMap()
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "DeleteKeySet"),
			zap.String("step", "deleteCachedKeySetMap"),
		}
		sb.logger.Error("error deleting keyset map", lf...)
		return errInternalServer("CreateKeySet", err)
	}

	sb.statsCollectionAction(collection, "delete", "solr.collection.create.error", time.Since(start))
	return nil
}

// ListKeySets - list all keysets
func (sb *SolrBackend) ListKeySets() ([]string, gobol.Error) {

	start := time.Now()

	keySetMap, err := sb.getCachedKeySetMap()
	if err != nil {
		sb.statsCollectionError("all", "list_cached", "memcached.collection.list.error")
		return nil, errInternalServer("ListKeySets", err)
	}

	if keySetMap != nil && len(keySetMap) > 0 {
		indexes := make([]string, 0, len(keySetMap))
		for k := range keySetMap {
			indexes = append(indexes, k)
		}

		sb.statsCollectionAction("all", "list_cached", "solr.collection.list", time.Since(start))
		return indexes, nil
	}

	indexes, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.list.error")
		return nil, errInternalServer("ListKeySets", e)
	}

	_, err = sb.cacheKeySetMap(indexes)
	if err != nil {
		sb.statsCollectionError("all", "list_cached", "memcached.collection.list.error")
		return nil, errInternalServer("ListKeySets", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.list", time.Since(start))
	return indexes, nil
}

// CheckKeySet - verifies if an index exists
func (sb *SolrBackend) CheckKeySet(keyset string) (bool, gobol.Error) {

	start := time.Now()

	keySetMap, err := sb.getCachedKeySetMap()
	if err != nil {
		sb.statsCollectionError("all", "list_cached", "memcached.collection.list.error")
		return false, errInternalServer("CheckKeySet", err)
	}

	if keySetMap != nil && len(keySetMap) > 0 {
		return keySetMap[keyset], nil
	}

	indexes, e := sb.solrService.ListCollections()
	if e != nil {
		sb.statsCollectionError("all", "list", "solr.collection.list.error")
		return false, errInternalServer("CheckKeySet", e)
	}

	keySetMap, err = sb.cacheKeySetMap(indexes)
	if err != nil {
		sb.statsCollectionError("all", "list_cached", "memcached.collection.list.error")
		return false, errInternalServer("CheckKeySet", e)
	}

	sb.statsCollectionAction("all", "list", "solr.collection.list", time.Since(start))
	return keySetMap[keyset], nil
}
