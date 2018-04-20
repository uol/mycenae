package metadata

import (
	"fmt"
	"regexp"
	"time"

	"github.com/uol/go-solr/solr"
	"github.com/uol/gobol"
	"github.com/uol/gobol/solar"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SolrBackend - struct
type SolrBackend struct {
	solrService       *solar.SolrService
	numShards         int
	replicationFactor int
	stats             *tsstats.StatsTS
	logger            *zap.Logger
	regexPattern      *regexp.Regexp
}

// NewSolrBackend - creates a new instance
func NewSolrBackend(settings *Settings, stats *tsstats.StatsTS, logger *zap.Logger) (*SolrBackend, error) {

	ss, err := solar.NewSolrService(settings.URL, logger)
	if err != nil {
		return nil, err
	}

	return &SolrBackend{
		solrService:       ss,
		stats:             stats,
		logger:            logger,
		replicationFactor: settings.ReplicationFactor,
		numShards:         settings.NumShards,
		regexPattern:      regexp.MustCompile("[\\{\\}\\*\\|\\$\\^\\?\\[\\]]+"),
	}, nil
}

// setupSchema - setups the schema for a new collection
func (sb *SolrBackend) setupSchema(collection string) error {

	err := sb.solrService.AddNewField(collection, "metric", "string", false, true, true, true)
	if err != nil {
		return err
	}

	sb.solrService.AddNewField(collection, "tagKey", "string", true, true, true, true)
	if err != nil {
		return err
	}

	sb.solrService.AddNewField(collection, "tagValue", "string", true, true, true, true)
	if err != nil {
		return err
	}

	sb.solrService.AddNewField(collection, "type", "string", false, true, true, false)
	if err != nil {
		return err
	}

	return nil
}

// CreateIndex - creates a new collection
func (sb *SolrBackend) CreateIndex(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.CreateCollection(collection, sb.numShards, sb.replicationFactor)
	if err != nil {
		sb.statsCollectionError(collection, "create", "solr.collection.action")
		return errInternalServer("CreateIndex", err)
	}

	err = sb.setupSchema(collection)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "createCollection"),
			zap.String("step", "setupSchema"),
		}
		sb.logger.Error("error on schema setup", lf...)
		return errInternalServer("CreateIndex", err)
	}

	sb.statsCollectionAction(collection, "create", "solr.collection.action", time.Since(start))

	return nil
}

// DeleteIndex - deletes a collection
func (sb *SolrBackend) DeleteIndex(collection string) gobol.Error {

	start := time.Now()
	err := sb.solrService.DeleteCollection(collection)
	if err != nil {
		sb.statsCollectionError(collection, "delete", "solr.collection.create.error")
		return errInternalServer("DeleteIndex", err)
	}
	sb.statsCollectionAction(collection, "delete", "solr.collection.create.error", time.Since(start))
	return nil
}

// removeRegexpSlashes - removes all regular expression slashes
func (sb *SolrBackend) removeRegexpSlashes(value string) string {
	length := len(value)
	if length >= 3 && string(value[0]) == "/" && string(value[length-1]) == "/" {
		runes := []rune(value)
		return string(runes[1 : length-1])
	}
	return value
}

// extractFacets - extract facets from the solr.SolrResult
func (sb *SolrBackend) extractFacets(r *solr.SolrResult, field, value string, size int, regex bool) []string {

	facets := []string{}

	wrapper := r.FacetCounts["facet_fields"]
	if wrapper == nil {
		return facets
	}

	facetFields := wrapper.(map[string]interface{})

	wrapper = facetFields[field]
	if wrapper == nil {
		return facets
	}

	rawValue := sb.removeRegexpSlashes(value)

	var regexValue *regexp.Regexp
	addAll := (value == "*")
	if regex {
		var err error
		regexValue, err = regexp.Compile(rawValue)
		if err != nil {
			lf := []zapcore.Field{
				zap.String("package", "metadata"),
				zap.String("func", "extractFacets"),
				zap.String("regexp", rawValue),
			}
			sb.logger.Error("error compiling regex", lf...)
		}
	} else {
		regexValue = nil
	}

	data := wrapper.([]interface{})
	for i := 0; i < len(data) && len(facets) < size; i += 2 {
		if data[i+1].(float64) > 0 {
			v := data[i].(string)
			if !regex {
				if addAll || rawValue == v {
					facets = append(facets, v)
				}
			} else if regexValue != nil && regexValue.MatchString(v) {
				facets = append(facets, v)
			}
		}
	}

	return facets
}

// filterFieldValues - filter by field value using wildcard
func (sb *SolrBackend) filterFieldValues(field, value, collection, action string, maxResults int) ([]string, int, gobol.Error) {

	var q string
	regex := false
	if sb.leaveEmpty(value) {
		q = "*:*"
	} else {
		regex = sb.regexPattern.MatchString(value)
		if regex {
			value = sb.SetRegexValue(value)
		}
		q = fmt.Sprintf("%s:%s", field, value)
	}

	r, err := sb.solrService.Facets(collection, q, "", 0, 0, field)
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search")
		return nil, 0, errInternalServer("filterFieldValues", err)
	}

	return sb.extractFacets(r, field, value, maxResults, regex), r.Results.NumFound, nil
}

// FilterTagValues - list all tag values from a collection
func (sb *SolrBackend) FilterTagValues(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	start := time.Now()
	tags, total, err := sb.filterFieldValues("tagValue", prefix, collection, "filter_tag_values", maxResults)
	if err != nil {
		sb.statsCollectionError(collection, "filter_tag_values", "solr.collection.search.error")
		return nil, 0, errInternalServer("FilterTagValues", err)
	}
	sb.statsCollectionAction(collection, "filter_tag_values", "solr.collection.search", time.Since(start))

	return tags, total, nil
}

// FilterTagKeys - list all tag keys from a collection
func (sb *SolrBackend) FilterTagKeys(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	start := time.Now()
	tags, total, err := sb.filterFieldValues("tagKey", prefix, collection, "filter_tag_keys", maxResults)
	if err != nil {
		sb.statsCollectionError(collection, "filter_tag_keys", "solr.collection.search.error")
		return nil, 0, errInternalServer("FilterTagKeys", err)
	}
	sb.statsCollectionAction(collection, "filter_tag_keys", "solr.collection.search", time.Since(start))

	return tags, total, nil
}

// FilterMetrics - list all metrics from a collection
func (sb *SolrBackend) FilterMetrics(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	start := time.Now()
	metrics, total, err := sb.filterFieldValues("metric", prefix, collection, "filter_metrics", maxResults)
	if err != nil {
		sb.statsCollectionError(collection, "filter_metrics", "solr.collection.search.error")
		return nil, 0, errInternalServer("ListMetrics", err)
	}
	sb.statsCollectionAction(collection, "filter_metrics", "solr.collection.search", time.Since(start))

	return metrics, total, nil
}

// SetRegexValue - add slashes to the value
func (sb *SolrBackend) SetRegexValue(value string) string {

	if value == "" {
		return value
	}

	return fmt.Sprintf("/%s/", value)
}

// leaveEmpty - checks if the value is '*' or empty
func (sb *SolrBackend) leaveEmpty(value string) bool {
	return value == "" || value == "*" || value == ".*"
}

// buildMetadataQuery - builds the metadata query
func (sb *SolrBackend) buildMetadataQuery(metadata *Metadata, tsType string) string {

	q := "type:" + tsType

	if !sb.leaveEmpty(metadata.Metric) {
		q += " AND metric:" + metadata.Metric
	}

	if metadata.TagKey != nil && len(metadata.TagKey) > 0 {

		tagKeyQ := ""

		for i := 0; i < len(metadata.TagKey); i++ {

			if !sb.leaveEmpty(metadata.TagKey[i]) {

				if len(tagKeyQ) > 0 {
					tagKeyQ += " OR "
				}

				if sb.regexPattern.MatchString(metadata.TagKey[i]) {
					metadata.TagKey[i] = sb.SetRegexValue(metadata.TagKey[i])
				}

				tagKeyQ += "tagKey:" + metadata.TagKey[i]
			}
		}

		if len(tagKeyQ) > 0 {
			q += " AND (" + tagKeyQ + ")"
		}
	}

	if metadata.TagValue != nil && len(metadata.TagValue) > 0 {

		tagValueQ := ""

		for i := 0; i < len(metadata.TagValue); i++ {

			if metadata.TagValue[i] != "" {

				if len(tagValueQ) > 0 {
					tagValueQ += " OR "
				}

				if sb.regexPattern.MatchString(metadata.TagValue[i]) {
					metadata.TagValue[i] = sb.SetRegexValue(metadata.TagValue[i])
				}

				tagValueQ += "tagValue:" + metadata.TagValue[i]
			}
		}

		if len(tagValueQ) > 0 {
			q += " AND (" + tagValueQ + ")"
		}
	}

	return q
}

// ListMetadata - list all metas from a collection
func (sb *SolrBackend) ListMetadata(collection, tsType string, includeMeta *Metadata, from, maxResults int) ([]Metadata, int, gobol.Error) {

	start := time.Now()

	q := sb.buildMetadataQuery(includeMeta, tsType)

	r, err := sb.solrService.SimpleQuery(collection, q, "", from, maxResults)
	if err != nil {
		sb.statsCollectionError(collection, "list_metas", "solr.collection.search.error")
		return nil, 0, errInternalServer("ListMetas", err)
	}

	sb.statsCollectionAction(collection, "list_metas", "solr.collection.search", time.Since(start))

	return sb.fromDocuments(r.Results), r.Results.NumFound, nil
}

// toDocuments - changes the metadata to the document format
func (sb *SolrBackend) toDocuments(metadatas []Metadata) []solr.Document {

	if metadatas == nil || len(metadatas) == 0 {
		return nil
	}

	docs := make([]solr.Document, len(metadatas))
	for i, meta := range metadatas {
		docs[i] = solr.Document{
			"id":       meta.ID,
			"metric":   meta.Metric,
			"tagKey":   meta.TagKey,
			"tagValue": meta.TagValue,
			"type":     meta.MetaType,
		}
	}

	return docs
}

// getArrayFromDocument - extracts the array from the document
func (sb *SolrBackend) getArrayFromDocument(key string, document *solr.Document) []string {

	rawArray := document.Get(key).([]interface{})
	stringArray := []string{}

	for i := 0; i < len(rawArray); i++ {
		stringArray = append(stringArray, rawArray[i].(string))
	}

	return stringArray
}

// fromDocuments - converts all documents to metadata format
func (sb *SolrBackend) fromDocuments(results *solr.Collection) []Metadata {

	if results == nil {
		return nil
	}

	docs := results.Docs

	if docs == nil || len(docs) == 0 {
		return nil
	}

	metadatas := make([]Metadata, len(docs))
	for i, doc := range docs {
		metadatas[i] = Metadata{
			ID:       doc.Get("id").(string),
			MetaType: doc.Get("type").(string),
			Metric:   doc.Get("metric").(string),
			TagKey:   sb.getArrayFromDocument("tagKey", &doc),
			TagValue: sb.getArrayFromDocument("tagValue", &doc),
		}
	}

	return metadatas
}

// AddDocuments - add/update a document or a series of documents
func (sb *SolrBackend) AddDocuments(collection string, metadatas []Metadata) error {

	start := time.Now()

	err := sb.solrService.AddDocuments(collection, true, sb.toDocuments(metadatas)...)
	if err != nil {
		sb.statsCollectionError(collection, "add_documents", "solr.collection.add")
		return errInternalServer("AddDocuments", err)
	}

	sb.statsCollectionAction(collection, "add_documents", "solr.collection.add", time.Since(start))

	return nil
}

// ListIndexes - list all indexes
func (sb *SolrBackend) ListIndexes() ([]string, error) {

	start := time.Now()

	indexes, err := sb.solrService.ListCollections()
	if err != nil {
		sb.statsCollectionError("all", "list_collections", "solr.collection.list.error")
		return nil, errInternalServer("AddDocuments", err)
	}

	sb.statsCollectionAction("all", "list_collections", "solr.collection.list", time.Since(start))

	return indexes, nil
}

// CheckMetadata - verifies if a metadata exists
func (sb *SolrBackend) CheckMetadata(collection, tsType, tsid string) (bool, error) {

	start := time.Now()

	q := fmt.Sprintf("id:%s AND type:%s", tsid, tsType)
	r, err := sb.solrService.SimpleQuery(collection, q, "", 0, 0)

	if err != nil {
		sb.statsCollectionError(collection, "check_metadata", "solr.collection.search.error")
		return false, errInternalServer("CheckMetadata", err)
	}

	sb.statsCollectionAction(collection, "check_metadata", "solr.collection.search", time.Since(start))

	return r.Results.NumFound > 0, nil
}

// Query - executes a raw query
func (sb *SolrBackend) Query(collection, query string, from, maxResults int) ([]Metadata, int, gobol.Error) {

	start := time.Now()

	r, err := sb.solrService.SimpleQuery(collection, query, "", from, maxResults)

	if err != nil {
		sb.statsCollectionError(collection, "query", "solr.collection.search.error")
		return nil, 0, errInternalServer("Query", err)
	}

	sb.statsCollectionAction(collection, "query", "solr.collection.search", time.Since(start))

	return sb.fromDocuments(r.Results), r.Results.NumFound, nil
}
