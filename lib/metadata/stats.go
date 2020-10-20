package metadata

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/utils"
)

/**
* Has functions to send data to the timeseries itself.
* @author rnojiri
**/

const (
	// metricSolrRequest - metric name for solr request
	metricSolrRequest string = "solr.request"

	// metricSolrRequestDuration - metric name for solr request duration
	metricSolrRequestDuration string = "solr.request.duration"

	// metricSolrRequestError - metric name for solr request errors
	metricSolrRequestError string = "solr.request.error"

	// metricMissedKeyset - metric name for validate keyset fail
	metricMissedKeyset string = "missed.keyset"

	// metricZeroKeysets - metric name for collections returned on solr request to list collection with zero length or nil value
	metricZeroKeysets string = "zero.keysets"

	// metricListCollectionsError - metric name for solr list collections request error
	metricListCollectionsError string = "solr.list.collections.error"
)

// solrOperation - identifies some solr operation
type solrOperation string

const (
	solrDocID      solrOperation = "doc_by_id"
	solrNewDoc     solrOperation = "new_doc"
	solrQuery      solrOperation = "query"
	solrFacetQuery solrOperation = "facet_query"
	solrDelete     solrOperation = "delete"
)

// statsError - stores an error statistics
func (sb *SolrBackend) statsError(function, collection, metaType string, operation solrOperation) {

	sb.timelineManager.FlattenCountIncN(
		function,
		metricSolrRequestError,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(collection),
		constants.StringsType, metaType,
		constants.StringsOperation, operation,
	)
}

// statsRequest - stores a sucessful request statistics
func (sb *SolrBackend) statsRequest(function, collection, metaType string, operation solrOperation, d time.Duration) {

	sb.timelineManager.FlattenMaxN(
		function,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		metricSolrRequestDuration,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(collection),
		constants.StringsType, metaType,
		constants.StringsOperation, operation,
	)

	sb.timelineManager.FlattenCountIncN(
		function,
		metricSolrRequest,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(collection),
		constants.StringsType, metaType,
		constants.StringsOperation, operation,
	)
}

// statsMissedKeyset - stores a missed keyset statistics
func (sb *SolrBackend) statsMissedKeyset(function, collection string) {

	sb.timelineManager.FlattenCountIncN(
		function,
		metricMissedKeyset,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(collection),
	)
}

// statsZeroKeysets - stores a listed collections length equal to zero statistics
func (sb *SolrBackend) statsZeroKeysets(function string) {

	sb.timelineManager.FlattenCountIncN(
		function,
		metricZeroKeysets,
	)
}

// statsListCollectionsError - stores a solr list collections request error
func (sb *SolrBackend) statsListCollectionsError(function string) {

	sb.timelineManager.FlattenCountIncN(
		function,
		metricListCollectionsError,
	)
}
