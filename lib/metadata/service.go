package metadata

import (
	"fmt"
	"regexp"
	"time"

	"github.com/uol/mycenae/lib/memcached"

	"github.com/uol/go-solr/solr"
	"github.com/uol/gobol"
	"github.com/uol/gobol/solar"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SolrBackend - struct
type SolrBackend struct {
	solrService           *solar.SolrService
	numShards             int
	replicationFactor     int
	regexPattern          *regexp.Regexp
	stats                 *tsstats.StatsTS
	logger                *zap.Logger
	memcached             *memcached.Memcached
	idCacheTTL            int32
	queryCacheTTL         int32
	keysetCacheTTL        int32
	fieldListQuery        string
	zookeeperConfig       string
	maxReturnedMetadata   int
	blacklistedKeysetMap  map[string]bool
	solrSpecialCharRegexp *regexp.Regexp
}

// NewSolrBackend - creates a new instance
func NewSolrBackend(settings *Settings, stats *tsstats.StatsTS, logger *zap.Logger, memcached *memcached.Memcached) (*SolrBackend, error) {

	ss, err := solar.NewSolrService(settings.URL, logger)
	if err != nil {
		return nil, err
	}

	baseWordRegexp := "[0-9A-Za-z\\-\\.\\_\\%\\&\\#\\;\\/\\?]+(\\{[0-9]+\\})?"
	rp := regexp.MustCompile("^\\.?\\*" + baseWordRegexp + "|" + baseWordRegexp + "\\.?\\*$|\\[" + baseWordRegexp + "\\][\\+\\*]{1}|\\(" + baseWordRegexp + "\\)|" + baseWordRegexp + "\\{[0-9]+\\}")

	blacklistedKeysetMap := map[string]bool{}
	for _, value := range settings.BlacklistedKeysets {
		blacklistedKeysetMap[value] = true
	}

	return &SolrBackend{
		solrService:           ss,
		stats:                 stats,
		logger:                logger,
		replicationFactor:     settings.ReplicationFactor,
		numShards:             settings.NumShards,
		regexPattern:          rp,
		memcached:             memcached,
		idCacheTTL:            settings.IDCacheTTL,
		queryCacheTTL:         settings.QueryCacheTTL,
		keysetCacheTTL:        settings.KeysetCacheTTL,
		fieldListQuery:        fmt.Sprintf("*,[child parentFilter=parent_doc:true limit=%d]", settings.MaxReturnedMetadata),
		zookeeperConfig:       settings.ZookeeperConfig,
		maxReturnedMetadata:   settings.MaxReturnedMetadata,
		blacklistedKeysetMap:  blacklistedKeysetMap,
		solrSpecialCharRegexp: regexp.MustCompile(`(\+|\-|\&|\||\!|\(|\)|\{|\}|\[|\]|\^|"|\~|\*|\?|\:|\\|/)`),
	}, nil
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

// HasRegexPattern - check if the value has a regular expression
func (sb *SolrBackend) HasRegexPattern(value string) bool {

	return sb.regexPattern.MatchString(value)
}

// extractFacets - extract facets from the solr.SolrResult
func (sb *SolrBackend) extractFacets(r *solr.SolrResult, field, value string) []string {

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
	regex := sb.regexPattern.MatchString(value)
	addAll := (value == "*")
	if regex {
		if !addAll {
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
			regexValue = regexp.MustCompile(".*")
		}
	} else {
		regexValue = nil
	}

	data := wrapper.([]interface{})
	for i := 0; i < len(data); i += 2 {
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

// cropFacets - crops to the desired size
func (sb *SolrBackend) cropFacets(facets []string, size int) []string {

	if size == 0 {
		return []string{}
	} else if len(facets) <= size {
		return facets
	}

	resized := make([]string, size)
	for i := 0; i < size; i++ {
		resized[i] = facets[i]
	}

	return resized
}

// filterFieldValues - filter by field value using wildcard
func (sb *SolrBackend) filterFieldValues(collection, action, field, value string, maxResults int) ([]string, int, gobol.Error) {

	var query Query
	var facetFields, childFacetFields []string
	isRegex := sb.regexPattern.MatchString(value)

	if field == "metric" {
		facetFields = []string{field}
		childFacetFields = nil
		query.Metric = value
		query.Regexp = isRegex
	} else {
		childFacetFields = []string{field}
		facetFields = nil
		query.Tags = make([]QueryTag, 1)
		query.Tags[0].Regexp = isRegex

		if field == "tag_key" {
			query.Tags[0].Key = value
		} else {
			query.Tags[0].Values = []string{value}
		}
	}

	facets, err := sb.getCachedFacets(collection, field, &query)
	if err != nil {
		sb.statsCollectionError(collection, action, "memcached.collection.search.error")
		return nil, 0, errInternalServer("filterFieldValues", err)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	q, _ := sb.buildMetadataQuery(&query, true)

	r, e := sb.solrService.Facets(collection, q, "", 0, 0, nil, facetFields, childFacetFields, true, sb.maxReturnedMetadata, 1)
	if e != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search")
		return nil, 0, errInternalServer("filterFieldValues", e)
	}

	facets = sb.extractFacets(r, field, value)

	err = sb.cacheFacets(facets, collection, field, &query)
	if err != nil {
		sb.statsCollectionError(collection, action, "memcached.collection.search.error")
		return nil, 0, errInternalServer("filterFieldValues", err)
	}

	cropped := sb.cropFacets(facets, maxResults)
	return cropped, len(facets), nil
}

// FilterTagValues - list all tag values from a collection
func (sb *SolrBackend) FilterTagValues(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	start := time.Now()
	tags, total, err := sb.filterFieldValues(collection, "filter_tag_values", "tag_value", prefix, maxResults)
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
	tags, total, err := sb.filterFieldValues(collection, "filter_tag_keys", "tag_key", prefix, maxResults)
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
	metrics, total, err := sb.filterFieldValues(collection, "filter_metrics", "metric", prefix, maxResults)
	if err != nil {
		sb.statsCollectionError(collection, "filter_metrics", "solr.collection.search.error")
		return nil, 0, errInternalServer("FilterMetrics", err)
	}
	sb.statsCollectionAction(collection, "filter_metrics", "solr.collection.search", time.Since(start))

	return metrics, total, nil
}

// SetRegexValue - add slashes to the value
func (sb *SolrBackend) SetRegexValue(value string) string {

	if value == "" || value == "*" {
		return value
	}

	return fmt.Sprintf("/%s/", value)
}

// leaveEmpty - checks if the value is '*' or empty
func (sb *SolrBackend) leaveEmpty(value string) bool {
	return value == "" || value == "*" || value == ".*"
}

// buildValuesGroup - builds a set of values to be searched
func (sb *SolrBackend) buildValuesGroup(field string, values []string, regexp bool) string {

	if values == nil {
		return ""
	}

	size := len(values)
	if size == 0 {
		return ""
	}

	qp := "{!parent which=\"parent_doc:true\"}"

	if size == 1 {

		if sb.leaveEmpty(values[0]) {
			return ""
		}

		if regexp {
			values[0] = sb.SetRegexValue(values[0])
		} else {
			values[0] = sb.escapeSolrSpecialChars(values[0])
		}

		return qp + field + ":" + values[0]
	}

	qp += field + ":("

	for i, value := range values {

		if regexp {
			value = sb.SetRegexValue(value)
		} else {
			value = sb.escapeSolrSpecialChars(value)
		}

		qp += value

		if i < size-1 {
			qp += " OR "
		}
	}

	qp += ")"

	return qp
}

// escapeSolrSpecialChars - replaces all Solr special characters
func (sb *SolrBackend) escapeSolrSpecialChars(value string) string {
	if value == "" || value == "*" {
		return value
	}
	return sb.solrSpecialCharRegexp.ReplaceAllString(value, "\\$1")
}

// buildMetadataQuery - builds the metadata query
func (sb *SolrBackend) buildMetadataQuery(query *Query, parentQueryOnly bool) (string, []string) {

	parentQuery := "{!parent which=\"parent_doc:true"

	if query.MetaType != "" {
		parentQuery += " AND type:" + query.MetaType
	}

	if !sb.leaveEmpty(query.Metric) {
		if query.Regexp {
			query.Metric = sb.SetRegexValue(query.Metric)
		} else {
			query.Metric = sb.escapeSolrSpecialChars(query.Metric)
		}
		parentQuery += " AND metric:" + query.Metric
	}

	parentQuery += "\"}"

	numTags := len(query.Tags)

	if parentQueryOnly {
		for i := 0; i < numTags; i++ {

			if i > 0 {
				parentQuery += "OR "
			}

			numValues := len(query.Tags[i].Values)

			if numValues > 0 {

				parentQuery += "(tag_value:("

				for j := 0; j < numValues; j++ {
					parentQuery += sb.escapeSolrSpecialChars(query.Tags[i].Values[j])
					if j < numValues-1 {
						parentQuery += " OR "
					}
				}

				parentQuery += ")"
			}

			if query.Tags[i].Key != "" {
				if query.Tags[i].Regexp {
					query.Tags[i].Key = sb.SetRegexValue(query.Tags[i].Key)
				} else {
					query.Tags[i].Key = sb.escapeSolrSpecialChars(query.Tags[i].Key)
				}
				if numValues > 0 {
					parentQuery += " AND "
				} else {
					parentQuery += "("
				}
				parentQuery += "tag_key:" + query.Tags[i].Key
			}

			parentQuery += ")"
		}

		return parentQuery, nil
	}

	filterQueries := []string{}

	for i := 0; i < numTags; i++ {

		numValues := len(query.Tags[i].Values)

		if !sb.leaveEmpty(query.Tags[i].Key) {
			if query.Tags[i].Regexp {
				query.Tags[i].Key = sb.SetRegexValue(query.Tags[i].Key)
			} else {
				query.Tags[i].Key = sb.escapeSolrSpecialChars(query.Tags[i].Key)
			}
			filterQueries = append(filterQueries, fmt.Sprintf("{!parent which=\"parent_doc:true\"}tag_key:%s", query.Tags[i].Key))
		}

		if query.Tags[i].Negate {
			for j := 0; j < numValues; j++ {
				if sb.leaveEmpty(query.Tags[i].Values[j]) {
					continue
				}
				if query.Tags[i].Regexp {
					query.Tags[i].Values[j] = sb.SetRegexValue(query.Tags[i].Values[j])
				} else {
					query.Tags[i].Values[j] = sb.escapeSolrSpecialChars(query.Tags[i].Values[j])
				}
				filterQueries = append(filterQueries, fmt.Sprintf("-({!parent which=\"parent_doc:true\"}tag_value:%s)", query.Tags[i].Values[j]))
			}
		} else {
			qf := sb.buildValuesGroup("tag_value", query.Tags[i].Values, query.Tags[i].Regexp)
			if qf != "" {
				filterQueries = append(filterQueries, qf)
			}
		}
	}

	return parentQuery, filterQueries
}

// FilterMetadata - list all metas from a collection
func (sb *SolrBackend) FilterMetadata(collection string, query *Query, from, maxResults int) ([]Metadata, int, gobol.Error) {

	start := time.Now()

	q, qfs := sb.buildMetadataQuery(query, false)

	r, err := sb.solrService.FilteredQuery(collection, q, sb.fieldListQuery, from, maxResults, qfs)
	if err != nil {
		sb.statsCollectionError(collection, "list_metas", "solr.collection.search.error")
		return nil, 0, errInternalServer("ListMetas", err)
	}

	sb.statsCollectionAction(collection, "list_metas", "solr.collection.search", time.Since(start))

	return sb.fromDocuments(r.Results), r.Results.NumFound, nil
}

// toDocuments - changes the metadata to the document format
func (sb *SolrBackend) toDocuments(metadatas []Metadata, collection string) (docs []solr.Document, ids []string) {

	total := len(metadatas)
	if total == 0 {
		return nil, nil
	}

	docs = make([]solr.Document, total)
	ids = make([]string, total)
	for i, meta := range metadatas {

		meta.Keyset = collection
		numTags := len(meta.TagKey)
		tagDocs := make([]solr.Document, numTags)
		ids[i] = meta.ID

		for j := 0; j < numTags; j++ {
			tagDocs[j] = solr.Document{
				"id":        fmt.Sprintf("%s-t%d", meta.ID, j),
				"tag_key":   meta.TagKey[j],
				"tag_value": meta.TagValue[j],
			}
		}

		docs[i] = solr.Document{
			"id":               meta.ID,
			"metric":           meta.Metric,
			"type":             meta.MetaType,
			"parent_doc":       true,
			"_childDocuments_": tagDocs,
		}
	}

	return docs, ids
}

// getTagKeysAndValues - extracts the array from the document
func (sb *SolrBackend) getTagKeysAndValues(document *solr.Document) ([]string, []string) {

	childDocs := document.Get("_childDocuments_")
	if childDocs == nil {

		lf := []zapcore.Field{
			zap.String("package", "metadata"),
			zap.String("func", "getTagKeysAndValues"),
		}

		sb.logger.Error(fmt.Sprintf("no \"_childDocuments_\": %v", document), lf...)

		return []string{}, []string{}
	}

	rawArray := childDocs.([]interface{})
	size := len(rawArray)
	keys := make([]string, size)
	values := make([]string, size)

	for i := 0; i < len(rawArray); i++ {
		rawTags := rawArray[i]
		if rawTags == nil {

			lf := []zapcore.Field{
				zap.String("package", "metadata"),
				zap.String("func", "getTagKeysAndValues"),
			}

			sb.logger.Error(fmt.Sprintf("no \"raw tags\": %v", document), lf...)

			continue
		}
		tagDoc := rawArray[i].(map[string]interface{})
		keys[i] = tagDoc["tag_key"].(string)
		values[i] = tagDoc["tag_value"].(string)
	}

	return keys, values
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

		keys, values := sb.getTagKeysAndValues(&doc)

		metadatas[i] = Metadata{
			ID:       doc.Get("id").(string),
			MetaType: doc.Get("type").(string),
			Metric:   doc.Get("metric").(string),
			TagKey:   keys,
			TagValue: values,
		}
	}

	return metadatas
}

// AddDocuments - add/update a document or a series of documents
func (sb *SolrBackend) AddDocuments(collection string, metadatas []Metadata) gobol.Error {

	start := time.Now()

	docs, ids := sb.toDocuments(metadatas, collection)

	lf := []zapcore.Field{
		zap.String("package", "metadata"),
		zap.String("func", "AddDocuments"),
		zap.String("collection", collection),
	}

	sb.logger.Info(fmt.Sprintf("adding documents: %v", ids), lf...)

	err := sb.solrService.AddDocuments(collection, true, docs...)
	if err != nil {
		sb.statsCollectionError(collection, "add_documents", "solr.collection.add")
		return errInternalServer("AddDocuments", err)
	}

	for i := 0; i < len(metadatas); i++ {
		go sb.cacheID(collection, metadatas[i].MetaType, metadatas[i].ID)
	}

	sb.statsCollectionAction(collection, "add_documents", "solr.collection.add", time.Since(start))

	return nil
}

// CheckMetadata - verifies if a metadata exists
func (sb *SolrBackend) CheckMetadata(collection, tsType, tsid string) (bool, gobol.Error) {

	isCached, err := sb.isIDCached(collection, tsType, tsid)
	if err != nil {
		return false, errInternalServer("CheckMetadata", err)
	}

	if isCached {
		return true, nil
	}

	start := time.Now()

	q := fmt.Sprintf("parent_doc:true AND id:%s AND type:%s", tsid, tsType)
	r, e := sb.solrService.SimpleQuery(collection, q, "", 0, 0)

	if e != nil {
		sb.statsCollectionError(collection, "check_metadata", "solr.collection.search.error")
		return false, errInternalServer("CheckMetadata", err)
	}

	sb.statsCollectionAction(collection, "check_metadata", "solr.collection.search", time.Since(start))

	if r.Results.NumFound > 0 {
		go sb.cacheID(collection, tsType, tsid)
		return true, nil
	}

	return false, nil
}

// DeleteDocumentByID - delete a document by ID and its child documents
func (sb *SolrBackend) DeleteDocumentByID(collection, tsType, id string) gobol.Error {

	start := time.Now()

	queryID := fmt.Sprintf("/%s.*/", id)

	err := sb.solrService.DeleteDocumentByID(collection, true, queryID)
	if err != nil {
		sb.statsCollectionError(collection, "delete_document", "solr.collection.delete")
		return errInternalServer("DeleteDocumentByID", err)
	}

	go sb.DeleteCachedIDifExist(collection, tsType, id)

	sb.statsCollectionAction(collection, "delete_document", "solr.collection.delete", time.Since(start))

	return nil
}

// DeleteCachedIDifExist - check if ID is cached and delete it
func (sb *SolrBackend) DeleteCachedIDifExist(collection, tsType, id string) gobol.Error {

	lf := []zapcore.Field{
		zap.String("package", "metadata"),
		zap.String("func", "DeleteCachedIDifExist"),
		zap.String("collection", collection),
		zap.String("tsType", tsType),
		zap.String("id", id),
	}

	isCached, er := sb.isIDCached(collection, tsType, id)
	if er != nil {
		sb.logger.Error("error getting tsid from the cache", lf...)
		return errInternalServer("DeleteCachedIDifExist", er)
	}

	if isCached {
		er = sb.deleteCachedID(collection, tsType, id)
		if er != nil {
			sb.logger.Error("error deleting tsid from cache", lf...)
			return errInternalServer("DeleteCachedIDifExist", er)
		}

		sb.logger.Info("deleted cached tsid", lf...)
	}

	return nil
}

// FilterTagValuesByMetricAndTag - returns all tag values related to the specified metric and tag
func (sb *SolrBackend) FilterTagValuesByMetricAndTag(collection, tsType, metric, tag, prefix string, maxResults int) ([]string, int, gobol.Error) {

	childrenQuery := "tag_key:" + tag

	return sb.filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, "filter_tag_values_by_metric_and_tag", "tag_value", "FilterTagValuesByMetricAndTag", maxResults)
}

// FilterTagKeysByMetric - returns all tag keys related to the specified metric
func (sb *SolrBackend) FilterTagKeysByMetric(collection, tsType, metric, prefix string, maxResults int) ([]string, int, gobol.Error) {

	childrenQuery := "tag_key:"

	if sb.HasRegexPattern(prefix) {
		childrenQuery += sb.SetRegexValue(prefix)
	} else {
		childrenQuery += sb.escapeSolrSpecialChars(prefix)
	}

	return sb.filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, "filter_tag_values_by_metric", "tag_key", "FilterTagKeysByMetric", maxResults)
}

// filterTagsByMetric - returns all tag keys or values related to the specified metric
func (sb *SolrBackend) filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, action, field, functionName string, maxResults int) ([]string, int, gobol.Error) {

	query := "{!parent which=\"parent_doc:true\"}" + childrenQuery
	filterQueries := []string{
		"type:" + tsType,
		"metric:" + metric,
	}

	start := time.Now()
	cacheKey := query + prefix

	facets, gerr := sb.getCachedFacets(collection, field, cacheKey)
	if gerr != nil {
		sb.statsCollectionError(collection, action, "memcached.collection.search.error")
		return nil, 0, errInternalServer(functionName, gerr)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	r, err := sb.solrService.Facets(collection, query, "", 0, 0, filterQueries, nil, []string{field}, true, maxResults, 0)
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search.error")
		return nil, 0, errInternalServer(functionName, err)
	}

	facets = sb.extractFacets(r, field, prefix)

	err = sb.cacheFacets(facets, collection, field, cacheKey)
	if err != nil {
		sb.statsCollectionError(collection, action, "memcached.collection.search.error")
		return nil, 0, errInternalServer(functionName, err)
	}

	cropped := sb.cropFacets(facets, maxResults)

	sb.statsCollectionAction(collection, action, "solr.collection.search", time.Since(start))

	return cropped, len(facets), nil
}
