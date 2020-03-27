package metadata

import (
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

	err := sb.solrService.CreateCollection(collection, sb.zookeeperConfig, sb.numShards, sb.replicationFactor)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "CreateKeyset").Msg("error on creating collection")
		}
		return errInternalServer("CreateKeyset", err)
	}

	sb.deleteCachedKeysets()

	return nil
}

// DeleteKeyset - deletes a collection
func (sb *SolrBackend) DeleteKeyset(collection string) gobol.Error {

	err := sb.solrService.DeleteCollection(collection)
	if err != nil {
		if logh.ErrorEnabled {
			sb.logger.Error().Err(err).Str(constants.StringsFunc, "DeleteKeyset").Msg("error deleting collection")
		}
		return errInternalServer("DeleteKeyset", err)
	}

	sb.deleteCachedKeysets()

	return nil
}

// ListKeysets - list all keysets
func (sb *SolrBackend) ListKeysets() []string {

	return sb.getCachedKeysets()
}

// CheckKeyset - verifies if an index exists
func (sb *SolrBackend) CheckKeyset(keyset string) bool {

	keysets := sb.getCachedKeysets()

	if len(keysets) > 0 {
		for _, k := range keysets {
			if k == keyset {
				return true
			}
		}
	}

	return false
}
