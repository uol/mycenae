package metadata

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/uol/logh"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/memcached"

	"github.com/uol/go-solr/solr"
	"github.com/uol/gobol"
	"github.com/uol/gobol/solar"
	"github.com/uol/mycenae/lib/tsstats"
)

// SolrBackend - struct
type SolrBackend struct {
	solrService                 *solar.SolrService
	numShards                   int
	replicationFactor           int
	regexPattern                *regexp.Regexp
	stats                       *tsstats.StatsTS
	logger                      *logh.ContextualLogger
	memcached                   *memcached.Memcached
	idCacheTTL                  int32
	queryCacheTTL               int32
	keysetCacheTTL              int32
	fieldListQuery              string
	zookeeperConfig             string
	maxReturnedMetadata         int
	blacklistedKeysetMap        map[string]bool
	solrSpecialCharRegexp       *regexp.Regexp
	solrRegexpSpecialCharRegexp *regexp.Regexp
	cacheKeyHashSize            int
}

// NewSolrBackend - creates a new instance
func NewSolrBackend(settings *Settings, stats *tsstats.StatsTS, memcached *memcached.Memcached) (*SolrBackend, error) {

	ss, err := solar.NewSolrService(settings.URL)
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
		solrService:                 ss,
		stats:                       stats,
		logger:                      logh.CreateContextualLogger(constants.StringsPKG, "metadata"),
		replicationFactor:           settings.ReplicationFactor,
		numShards:                   settings.NumShards,
		regexPattern:                rp,
		memcached:                   memcached,
		idCacheTTL:                  settings.IDCacheTTL,
		queryCacheTTL:               settings.QueryCacheTTL,
		keysetCacheTTL:              settings.KeysetCacheTTL,
		fieldListQuery:              fmt.Sprintf("*,[child parentFilter=parent_doc:true limit=%d]", settings.MaxReturnedMetadata),
		zookeeperConfig:             settings.ZookeeperConfig,
		maxReturnedMetadata:         settings.MaxReturnedMetadata,
		blacklistedKeysetMap:        blacklistedKeysetMap,
		solrSpecialCharRegexp:       regexp.MustCompile(`(\+|\-|\&|\||\!|\(|\)|\{|\}|\[|\]|\^|"|\~|\*|\?|\:|\/|\\)`),
		solrRegexpSpecialCharRegexp: regexp.MustCompile(`(\/)`),
		cacheKeyHashSize:            settings.CacheKeyHashSize,
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
func (sb *SolrBackend) extractFacets(r *solr.SolrResult, field, value, collection string) []string {

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
				if logh.ErrorEnabled {
					sb.log(sb.logger.Error(), "extractFacets", collection).Err(err).Msg("error compiling regex")
				}
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

	q, _ := sb.buildMetadataQuery(&query, true)

	facets, err := sb.getCachedFacets(collection, q)
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search.error")
		return nil, 0, errInternalServer("filterFieldValues", err)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	r, e := sb.solrService.Facets(collection, q, constants.StringsEmpty, 0, 0, nil, facetFields, childFacetFields, true, sb.maxReturnedMetadata, 1)
	if e != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search")
		return nil, 0, errInternalServer("filterFieldValues", e)
	}

	facets = sb.extractFacets(r, field, value, collection)

	err = sb.cacheFacets(facets, collection, q)
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.search.error")
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

	if value == constants.StringsEmpty || value == "*" {
		return value
	}

	return fmt.Sprintf("/%s/", sb.solrRegexpSpecialCharRegexp.ReplaceAllString(value, "\\$1"))
}

// leaveEmpty - checks if the value is '*' or empty
func (sb *SolrBackend) leaveEmpty(value string) bool {
	return value == constants.StringsEmpty || value == "*" || value == ".*"
}

// buildValuesGroup - builds a set of values to be searched
func (sb *SolrBackend) buildValuesGroup(field string, values []string, regexp bool) string {

	if values == nil {
		return constants.StringsEmpty
	}

	size := len(values)
	if size == 0 {
		return constants.StringsEmpty
	}

	qp := "{!parent which=\"parent_doc:true\"}"

	if size == 1 {

		if sb.leaveEmpty(values[0]) {
			return constants.StringsEmpty
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
	if value == constants.StringsEmpty || value == "*" {
		return value
	}
	return sb.solrSpecialCharRegexp.ReplaceAllString(value, "\\$1")
}

// buildMetadataQuery - builds the metadata query
func (sb *SolrBackend) buildMetadataQuery(query *Query, parentQueryOnly bool) (string, []string) {

	parentQuery := "{!parent which=\"parent_doc:true"

	if query.MetaType != constants.StringsEmpty {
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

			if query.Tags[i].Key != constants.StringsEmpty {
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
			if qf != constants.StringsEmpty {
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
		return nil, 0, errInternalServer("FilterMetadata", err)
	}

	sb.statsCollectionAction(collection, "list_metas", "solr.collection.search", time.Since(start))

	return sb.fromDocuments(r.Results, collection), r.Results.NumFound, nil
}

// toDocument - changes the metadata to the document format
func (sb *SolrBackend) toDocument(metadata *Metadata, collection string) (docs *solr.Document, id string) {

	if metadata == nil {
		return nil, constants.StringsEmpty
	}

	metadata.Keyset = collection
	numTags := len(metadata.TagKey)
	tagDocs := make([]solr.Document, numTags)

	for j := 0; j < numTags; j++ {
		tagDocs[j] = solr.Document{
			"id":        fmt.Sprintf("%s-t%d", metadata.ID, j),
			"tag_key":   metadata.TagKey[j],
			"tag_value": metadata.TagValue[j],
		}
	}

	doc := &solr.Document{
		"id":               metadata.ID,
		"metric":           metadata.Metric,
		"type":             metadata.MetaType,
		"parent_doc":       true,
		"_childDocuments_": tagDocs,
	}

	return doc, metadata.ID
}

// getTagKeysAndValues - extracts the array from the document
func (sb *SolrBackend) getTagKeysAndValues(document *solr.Document, collection string) ([]string, []string) {

	childDocs := document.Get("_childDocuments_")
	if childDocs == nil {
		if logh.ErrorEnabled {
			sb.log(sb.logger.Error(), "getTagKeysAndValues", collection).Msgf("no \"_childDocuments_\": %v", document)
		}

		return []string{}, []string{}
	}

	rawArray := childDocs.([]interface{})
	size := len(rawArray)
	keys := make([]string, size)
	values := make([]string, size)

	for i := 0; i < len(rawArray); i++ {
		rawTags := rawArray[i]
		if rawTags == nil {
			if logh.ErrorEnabled {
				sb.log(sb.logger.Error(), "getTagKeysAndValues", collection).Msgf("no \"raw tags\": %v", document)
			}

			continue
		}
		tagDoc := rawArray[i].(map[string]interface{})
		keys[i] = tagDoc["tag_key"].(string)
		values[i] = tagDoc["tag_value"].(string)
	}

	return keys, values
}

// fromDocuments - converts all documents to metadata format
func (sb *SolrBackend) fromDocuments(results *solr.Collection, collection string) []Metadata {

	if results == nil {
		return nil
	}

	docs := results.Docs

	if docs == nil || len(docs) == 0 {
		return nil
	}

	metadatas := make([]Metadata, len(docs))
	for i, doc := range docs {

		keys, values := sb.getTagKeysAndValues(&doc, collection)

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

// log - add the common log fields
func (sb *SolrBackend) log(event *zerolog.Event, funcName, keyset string) *zerolog.Event {
	return event.Str(constants.StringsFunc, funcName).Str(constants.StringsKeyset, keyset)
}

// AddDocument - add/update a document
func (sb *SolrBackend) AddDocument(collection string, metadata *Metadata) gobol.Error {

	start := time.Now()

	doc, id := sb.toDocument(metadata, collection)

	if logh.InfoEnabled {
		sb.log(sb.logger.Info(), "AddDocument", collection).Msgf("adding document: %s", id)
	}

	err := sb.solrService.AddDocument(collection, true, doc)
	if err != nil {
		sb.statsCollectionError(collection, "add_documents", "solr.collection.error")
		return errInternalServer("AddDocument", err)
	}

	go sb.cacheID(collection, metadata.MetaType, metadata.ID)

	sb.statsCollectionAction(collection, "add_documents", "solr.collection", time.Since(start))

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
	r, e := sb.solrService.SimpleQuery(collection, q, constants.StringsEmpty, 0, 0)

	if e != nil {
		sb.statsCollectionError(collection, "check_metadata", "solr.collection.error")
		return false, errInternalServer("CheckMetadata", err)
	}

	sb.statsCollectionAction(collection, "check_metadata", "solr.collection", time.Since(start))

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
		sb.statsCollectionError(collection, "delete_document", "solr.collection.error")
		return errInternalServer("DeleteDocumentByID", err)
	}

	go sb.DeleteCachedIDifExist(collection, tsType, id)

	sb.statsCollectionAction(collection, "delete_document", "solr.collection", time.Since(start))

	return nil
}

// DeleteCachedIDifExist - check if ID is cached and delete it
func (sb *SolrBackend) DeleteCachedIDifExist(collection, tsType, id string) gobol.Error {

	isCached, err := sb.isIDCached(collection, tsType, id)
	if err != nil {
		if logh.ErrorEnabled {
			sb.log(sb.logger.Error(), "DeleteCachedIDifExist", collection).Err(err).Msg("error getting tsid from the cache")
		}
		return errInternalServer("DeleteCachedIDifExist", err)
	}

	if isCached {
		err = sb.deleteCachedID(collection, tsType, id)
		if err != nil {
			if logh.ErrorEnabled {
				sb.log(sb.logger.Error(), "DeleteCachedIDifExist", collection).Err(err).Msg("error deleting tsid from cache")
			}
			return errInternalServer("DeleteCachedIDifExist", err)
		}

		if logh.DebugEnabled {
			sb.log(sb.logger.Debug(), "DeleteCachedIDifExist", collection).Msg("deleted cached tsid")
		}
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

	maxResultsStr := strconv.Itoa(maxResults)
	strBuilder := strings.Builder{}
	strBuilder.Grow(len(query) + len(filterQueries[0]) + len(filterQueries[1]) + len(maxResultsStr) + 3)
	strBuilder.WriteString(query)
	strBuilder.WriteString(constants.StringsBar)
	strBuilder.WriteString(filterQueries[0])
	strBuilder.WriteString(constants.StringsBar)
	strBuilder.WriteString(filterQueries[1])
	strBuilder.WriteString(constants.StringsBar)
	strBuilder.WriteString(strconv.Itoa(maxResults))

	start := time.Now()

	facets, gerr := sb.getCachedFacets(collection, strBuilder.String())
	if gerr != nil {
		sb.statsCollectionError(collection, action, "solr.collection.error")
		return nil, 0, errInternalServer(functionName, gerr)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	r, err := sb.solrService.Facets(collection, query, constants.StringsEmpty, 0, 0, filterQueries, nil, []string{field}, true, maxResults, 0)
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.error")
		return nil, 0, errInternalServer(functionName, err)
	}

	facets = sb.extractFacets(r, field, prefix, collection)

	err = sb.cacheFacets(facets, collection, strBuilder.String())
	if err != nil {
		sb.statsCollectionError(collection, action, "solr.collection.error")
		return nil, 0, errInternalServer(functionName, err)
	}

	cropped := sb.cropFacets(facets, maxResults)

	sb.statsCollectionAction(collection, action, "solr.collection", time.Since(start))

	return cropped, len(facets), nil
}
