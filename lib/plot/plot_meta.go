package plot

import (
	"github.com/uol/logh"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

func (plot *Plot) validateKeyset(keyset string) gobol.Error {

	found := plot.persist.metaStorage.CheckKeyset(keyset)
	if !found {
		return errKeysetNotFound("validateKeyset")
	}

	return nil
}

func (plot *Plot) checkParams(from, size int) (int, int) {

	if from < 0 {
		from = 0
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return from, size
}

const (
	cFuncCheckTotalTSLimits string = "checkTotalTSLimits"
	cMsgCheckTotalTSLimits  string = "maximum allowed number of timeseries"
)

func (plot *Plot) checkTotalTSLimits(message, keyset, metric string, total int) gobol.Error {

	if total > plot.LogQueryTSThreshold {
		plot.statsQueryTSThreshold(cFuncCheckTotalTSLimits, keyset, total)
		if logh.WarnEnabled {
			plot.logger.Warn().Str(constants.StringsFunc, cFuncCheckTotalTSLimits).Str("type", "q").Str(constants.StringsKeyset, keyset).Str("metric", metric).Msg(message)
		}
	}

	if total > plot.MaxTimeseries {
		plot.statsQueryTSLimit(cFuncCheckTotalTSLimits, keyset, total)
		if logh.WarnEnabled {
			plot.logger.Warn().Str(constants.StringsFunc, cFuncCheckTotalTSLimits).Str("type", "ts").Str(constants.StringsKeyset, keyset).Str("metric", metric).Msg(message)
		}

		return errValidationS(
			cFuncCheckTotalTSLimits,
			cMsgCheckTotalTSLimits,
		)
	}
	return nil
}

func (plot *Plot) FilterMetrics(keyset, metricName string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeyset(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterMetrics")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterMetrics(keyset, metricName, size)
}

func (plot *Plot) FilterTagKeys(keyset, tagKname string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeyset(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterTagKeys")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterTagKeys(keyset, tagKname, size)
}

func (plot *Plot) FilterTagValues(keyset, tagVname string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeyset(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterTagValues")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterTagValues(keyset, tagVname, size)
}

// toMetaParam - converts metric and tags to a Metadata struct to be used as query
func (plot *Plot) toMetaParam(metric, tsType string, tags map[string]string) *metadata.Query {

	q := &metadata.Query{
		Metric:   metric,
		MetaType: tsType,
		Regexp:   plot.persist.metaStorage.HasRegexPattern(metric),
	}

	size := len(tags)
	if size > 0 {
		q.Tags = make([]metadata.QueryTag, size)

		i := 0
		for k, v := range tags {
			q.Tags[i] = metadata.QueryTag{
				Key:    k,
				Values: []string{v},
				Negate: false,
				Regexp: plot.persist.metaStorage.HasRegexPattern(k) || plot.persist.metaStorage.HasRegexPattern(v),
			}
			i++
		}
	}

	return q
}

// toMetaParam - converts metric and tags to a Metadata struct to be used as query
func (plot *Plot) toMetaParamArray(metric, tsType string, tags map[string][]string) *metadata.Query {

	q := &metadata.Query{
		Metric:   metric,
		MetaType: tsType,
		Regexp:   plot.persist.metaStorage.HasRegexPattern(metric),
	}

	size := len(tags)
	if size > 0 {
		q.Tags = make([]metadata.QueryTag, size)

		i := 0
		for k, vs := range tags {

			hasRegex := plot.persist.metaStorage.HasRegexPattern(k)
			for _, v := range vs {
				hasRegex = hasRegex || plot.persist.metaStorage.HasRegexPattern(v)
			}

			q.Tags[i] = metadata.QueryTag{
				Key:    k,
				Values: vs,
				Negate: false,
				Regexp: hasRegex,
			}
			i++
		}
	}

	return q
}

// extractTagMap - extracts all tags and tag values to
func (plot *Plot) extractTagMap(metadata *metadata.Metadata) map[string]string {

	tagMap := map[string]string{}
	for i := 0; i < len(metadata.TagKey); i++ {
		tagMap[metadata.TagKey[i]] = metadata.TagValue[i]
	}

	return tagMap
}

func (plot *Plot) ListMeta(keyset, tsType, metric string, tags map[string]string, onlyids bool, size, from int) ([]TsMetaInfo, int, gobol.Error) {

	from, size = plot.checkParams(from, size)

	metadatas, total, gerr := plot.persist.metaStorage.FilterMetadata(keyset, plot.toMetaParam(metric, tsType, tags), from, size)

	var tsMetaInfos []TsMetaInfo

	for _, metadata := range metadatas {

		var tsmi TsMetaInfo

		if !onlyids {

			tagMap := plot.extractTagMap(&metadata)

			tsmi = TsMetaInfo{
				Metric: metadata.Metric,
				TsId:   metadata.ID,
				Tags:   tagMap,
			}
		} else {
			tsmi = TsMetaInfo{
				TsId: metadata.ID,
			}
		}

		tsMetaInfos = append(tsMetaInfos, tsmi)

	}

	return tsMetaInfos, total, gerr
}
