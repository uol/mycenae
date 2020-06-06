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
	tlmanager "github.com/uol/timelinemanager"

	"github.com/uol/go-solr/solr"
	"github.com/uol/gobol"
	"github.com/uol/gobol/solar"
)

// SolrBackend - struct
type SolrBackend struct {
	solrService                   *solar.SolrService
	numShards                     int
	replicationFactor             int
	regexPattern                  *regexp.Regexp
	timelineManager               *tlmanager.Instance
	logger                        *logh.ContextualLogger
	memcached                     *memcached.Memcached
	idCacheTTL                    []byte
	noIDCache                     bool
	queryCacheTTL                 []byte
	noQueryCache                  bool
	fieldListQuery                string
	zookeeperConfig               string
	maxReturnedMetadata           int
	blacklistedKeysetMap          map[string]bool
	solrSpecialCharRegexp         *regexp.Regexp
	solrRegexpSpecialCharRegexp   *regexp.Regexp
	cacheKeyHashSize              int
	cachedKeysets                 []string
	keysetCacheAutoUpdateInterval time.Duration
}

// NewSolrBackend - creates a new instance
func NewSolrBackend(settings *Settings, mc *tlmanager.Instance, memcached *memcached.Memcached) (*SolrBackend, error) {

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

	keysetCacheAutoUpdateIntervalDuration, err := time.ParseDuration(settings.KeysetCacheAutoUpdateInterval)
	if err != nil {
		return nil, fmt.Errorf("error parsing keysetCacheAutoUpdateInterval")
	}

	logger := logh.CreateContextualLogger(constants.StringsPKG, "metadata")

	if logh.InfoEnabled {
		logger.Info().Msgf("setting keyset cache auto update interval to: %s", settings.KeysetCacheAutoUpdateInterval)
	}

	sb := &SolrBackend{
		solrService:                   ss,
		timelineManager:               mc,
		logger:                        logger,
		replicationFactor:             settings.ReplicationFactor,
		numShards:                     settings.NumShards,
		regexPattern:                  rp,
		memcached:                     memcached,
		idCacheTTL:                    []byte(strconv.Itoa(settings.IDCacheTTL)),
		noIDCache:                     settings.IDCacheTTL < 0,
		queryCacheTTL:                 []byte(strconv.Itoa(settings.QueryCacheTTL)),
		noQueryCache:                  settings.QueryCacheTTL < 0,
		fieldListQuery:                fmt.Sprintf("*,[child parentFilter=parent_doc:true limit=%d]", settings.MaxReturnedMetadata),
		zookeeperConfig:               settings.ZookeeperConfig,
		maxReturnedMetadata:           settings.MaxReturnedMetadata,
		blacklistedKeysetMap:          blacklistedKeysetMap,
		solrSpecialCharRegexp:         regexp.MustCompile(`(\+|\-|\&|\||\!|\(|\)|\{|\}|\[|\]|\^|"|\~|\*|\?|\:|\/|\\)`),
		solrRegexpSpecialCharRegexp:   regexp.MustCompile(`(\/)`),
		cacheKeyHashSize:              settings.CacheKeyHashSize,
		keysetCacheAutoUpdateInterval: keysetCacheAutoUpdateIntervalDuration,
	}

	sb.cacheKeysets()
	sb.autoUpdateCachedKeysets()

	return sb, nil
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
func (sb *SolrBackend) filterFieldValues(function, collection, field, value string, maxResults int) ([]string, int, gobol.Error) {

	start := time.Now()

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
		sb.statsError(function, collection, constants.StringsAll, solrFacetQuery)
		return nil, 0, errInternalServer(function, err)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	r, e := sb.solrService.Facets(collection, q, constants.StringsEmpty, 0, 0, nil, facetFields, childFacetFields, true, sb.maxReturnedMetadata, 1)
	if e != nil {
		sb.statsError(function, collection, constants.StringsAll, solrFacetQuery)
		return nil, 0, errInternalServer(function, e)
	}

	facets = sb.extractFacets(r, field, value, collection)

	err = sb.cacheFacets(facets, collection, q)
	if err != nil {
		sb.statsError(function, collection, constants.StringsAll, solrFacetQuery)
		return nil, 0, errInternalServer(function, err)
	}

	cropped := sb.cropFacets(facets, maxResults)

	sb.statsRequest(function, collection, constants.StringsAll, solrFacetQuery, time.Since(start))

	return cropped, len(facets), nil
}

const funcFilterTagValues string = "FilterTagValues"

// FilterTagValues - list all tag values from a collection
func (sb *SolrBackend) FilterTagValues(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	tags, total, err := sb.filterFieldValues(funcFilterTagValues, collection, "tag_value", prefix, maxResults)
	if err != nil {
		return nil, 0, errInternalServer(funcFilterTagValues, err)
	}

	return tags, total, nil
}

const funcFilterTagKeys string = "FilterTagKeys"

// FilterTagKeys - list all tag keys from a collection
func (sb *SolrBackend) FilterTagKeys(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	tags, total, err := sb.filterFieldValues(funcFilterTagKeys, collection, "tag_key", prefix, maxResults)
	if err != nil {
		return nil, 0, errInternalServer("FilterTagKeys", err)
	}

	return tags, total, nil
}

const funcFilterMetrics string = "FilterMetrics"

// FilterMetrics - list all metrics from a collection
func (sb *SolrBackend) FilterMetrics(collection, prefix string, maxResults int) ([]string, int, gobol.Error) {

	metrics, total, err := sb.filterFieldValues(funcFilterMetrics, collection, "metric", prefix, maxResults)
	if err != nil {
		return nil, 0, errInternalServer("FilterMetrics", err)
	}

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

const funcFilterMetadata string = "FilterMetadata"

// FilterMetadata - list all metas from a collection
func (sb *SolrBackend) FilterMetadata(collection string, query *Query, from, maxResults int) ([]Metadata, int, gobol.Error) {

	start := time.Now()

	q, qfs := sb.buildMetadataQuery(query, false)

	r, err := sb.solrService.FilteredQuery(collection, q, sb.fieldListQuery, from, maxResults, qfs)
	if err != nil {
		sb.statsError(funcFilterMetadata, collection, query.MetaType, solrQuery)
		return nil, 0, errInternalServer(funcFilterMetadata, err)
	}

	sb.statsRequest(funcFilterMetadata, collection, query.MetaType, solrQuery, time.Since(start))

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

const funcAddDocument string = "AddDocument"

// AddDocument - add/update a document
func (sb *SolrBackend) AddDocument(collection string, m *Metadata) gobol.Error {

	start := time.Now()

	doc, id := sb.toDocument(m, collection)

	if logh.InfoEnabled {
		sb.log(sb.logger.Info(), funcAddDocument, collection).Msgf("adding document: %s", id)
	}

	err := sb.solrService.AddDocument(collection, true, doc)
	if err != nil {
		sb.statsError(funcAddDocument, collection, m.MetaType, solrNewDoc)
		return errInternalServer(funcAddDocument, err)
	}

	go sb.cacheID(collection, m.MetaType, m.ID, []byte(m.ID))

	sb.statsRequest(funcAddDocument, collection, m.MetaType, solrNewDoc, time.Since(start))

	return nil
}

const (
	funcCheckMetadata  string = "CheckMetadata"
	queryCheckMetadata string = "parent_doc:true AND id:%s AND type:%s"
)

// CheckMetadata - verifies if a metadata exists
func (sb *SolrBackend) CheckMetadata(collection, tsType, tsid string, tsidBytes []byte) (bool, gobol.Error) {

	isCached, err := sb.isIDCached(collection, tsType, tsid, tsidBytes)
	if err != nil {
		return false, errInternalServer(funcCheckMetadata, err)
	}

	if isCached {
		return true, nil
	}

	start := time.Now()

	q := fmt.Sprintf(queryCheckMetadata, tsid, tsType)
	r, e := sb.solrService.SimpleQuery(collection, q, constants.StringsEmpty, 0, 0)

	if e != nil {
		sb.statsError(funcCheckMetadata, collection, tsType, solrDocID)
		return false, errInternalServer(funcCheckMetadata, err)
	}

	sb.statsRequest(funcCheckMetadata, collection, tsType, solrDocID, time.Since(start))

	if r.Results.NumFound > 0 {
		go sb.cacheID(collection, tsType, tsid, tsidBytes)
		return true, nil
	}

	return false, nil
}

const (
	funcDeleteDocumentByID  string = "DeleteDocumentByID"
	queryDeleteDocumentByID string = "/%s.*/"
)

// DeleteDocumentByID - delete a document by ID and its child documents
func (sb *SolrBackend) DeleteDocumentByID(collection, tsType, id string) gobol.Error {

	start := time.Now()

	queryID := fmt.Sprintf(queryDeleteDocumentByID, id)

	err := sb.solrService.DeleteDocumentByID(collection, true, queryID)
	if err != nil {
		sb.statsError(funcDeleteDocumentByID, collection, tsType, solrDelete)
		return errInternalServer(funcDeleteDocumentByID, err)
	}

	go sb.DeleteCachedIDifExist(collection, tsType, id)

	sb.statsRequest(funcDeleteDocumentByID, collection, tsType, solrDelete, time.Since(start))

	return nil
}

const funcDeleteCachedIDifExist string = "DeleteCachedIDifExist"

// DeleteCachedIDifExist - check if ID is cached and delete it
func (sb *SolrBackend) DeleteCachedIDifExist(collection, tsType, id string) gobol.Error {

	isCached, err := sb.isIDCached(collection, tsType, id, []byte(id))
	if err != nil {
		if logh.ErrorEnabled {
			sb.log(sb.logger.Error(), funcDeleteCachedIDifExist, collection).Err(err).Msg("error getting tsid from the cache")
		}
		return errInternalServer(funcDeleteCachedIDifExist, err)
	}

	if isCached {
		err = sb.deleteCachedID(collection, tsType, id)
		if err != nil {
			if logh.ErrorEnabled {
				sb.log(sb.logger.Error(), funcDeleteCachedIDifExist, collection).Err(err).Msg("error deleting tsid from cache")
			}
			return errInternalServer(funcDeleteCachedIDifExist, err)
		}

		if logh.DebugEnabled {
			sb.log(sb.logger.Debug(), funcDeleteCachedIDifExist, collection).Msg("deleted cached tsid")
		}
	}

	return nil
}

const funcFilterTagValuesByMetricAndTag string = "FilterTagValuesByMetricAndTag"

// FilterTagValuesByMetricAndTag - returns all tag values related to the specified metric and tag
func (sb *SolrBackend) FilterTagValuesByMetricAndTag(collection, tsType, metric, tag, prefix string, maxResults int) ([]string, int, gobol.Error) {

	childrenQuery := "tag_key:" + tag

	return sb.filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, "tag_value", funcFilterTagValuesByMetricAndTag, maxResults)
}

const funcFilterTagKeysByMetric string = "FilterTagKeysByMetric"

// FilterTagKeysByMetric - returns all tag keys related to the specified metric
func (sb *SolrBackend) FilterTagKeysByMetric(collection, tsType, metric, prefix string, maxResults int) ([]string, int, gobol.Error) {

	childrenQuery := "tag_key:"

	if sb.HasRegexPattern(prefix) {
		childrenQuery += sb.SetRegexValue(prefix)
	} else {
		childrenQuery += sb.escapeSolrSpecialChars(prefix)
	}

	return sb.filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, "tag_key", funcFilterTagKeysByMetric, maxResults)
}

// filterTagsByMetric - returns all tag keys or values related to the specified metric
func (sb *SolrBackend) filterTagsByMetric(collection, tsType, metric, childrenQuery, prefix, field, functionName string, maxResults int) ([]string, int, gobol.Error) {

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
		sb.statsError(functionName, collection, tsType, solrQuery)
		return nil, 0, errInternalServer(functionName, gerr)
	}

	if facets != nil && len(facets) > 0 {
		cropped := sb.cropFacets(facets, maxResults)
		return cropped, len(facets), nil
	}

	r, err := sb.solrService.Facets(collection, query, constants.StringsEmpty, 0, 0, filterQueries, nil, []string{field}, true, maxResults, 0)
	if err != nil {
		sb.statsError(functionName, collection, tsType, solrQuery)
		return nil, 0, errInternalServer(functionName, err)
	}

	facets = sb.extractFacets(r, field, prefix, collection)

	err = sb.cacheFacets(facets, collection, strBuilder.String())
	if err != nil {
		sb.statsError(functionName, collection, tsType, solrQuery)
		return nil, 0, errInternalServer(functionName, err)
	}

	cropped := sb.cropFacets(facets, maxResults)

	sb.statsRequest(functionName, collection, tsType, solrQuery, time.Since(start))

	return cropped, len(facets), nil
}
