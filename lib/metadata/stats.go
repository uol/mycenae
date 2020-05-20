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
