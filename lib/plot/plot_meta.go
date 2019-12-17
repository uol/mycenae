package plot

import (
	"fmt"

	"github.com/uol/gobol/logh"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

func (plot *Plot) validateKeySet(keyset string) gobol.Error {

	found, gerr := plot.persist.metaStorage.CheckKeySet(keyset)
	if gerr != nil {
		return gerr
	}
	if !found {
		return errNotFound("validateKeySet")
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

func (plot *Plot) checkTotalTSLimits(message, keyset, metric string, total int) gobol.Error {

	if total > plot.LogQueryTSThreshold {
		plot.statsQueryTSThreshold(keyset, total)
		if logh.WarnEnabled {
			plot.logger.Warn().Str(constants.StringsFunc, "checkTotalTSLimits").Str("keyset", keyset).Str("metric", metric).Int("total", total).Int("configured", plot.LogQueryTSThreshold).Msgf("reaching max timeseries: ", message)
		}
	}

	if total > plot.MaxTimeseries {
		plot.statsQueryTSLimit(keyset, total)
		if logh.ErrorEnabled {
			plot.logger.Error().Str(constants.StringsFunc, "checkTotalTSLimits").Str("keyset", keyset).Str("metric", metric).Int("total", total).Int("configured", plot.LogQueryTSThreshold).Msgf("max timeseries reached", message)
		}

		return errValidationS(
			"checkTotalLimits",
			fmt.Sprintf(
				"query exceeded the maximum allowed number of timeseries. max is %d and the query returned %d",
				plot.MaxTimeseries,
				total,
			),
		)
	}
	return nil
}

func (plot *Plot) FilterMetrics(keyset, metricName string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeySet(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterMetrics")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterMetrics(keyset, metricName, size)
}

func (plot *Plot) FilterTagKeys(keyset, tagKname string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeySet(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterTagKeys")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterTagKeys(keyset, tagKname, size)
}

func (plot *Plot) FilterTagValues(keyset, tagVname string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeySet(keyset)
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

func (plot *Plot) ListMeta(keySet, tsType, metric string, tags map[string]string, onlyids bool, size, from int) ([]TsMetaInfo, int, gobol.Error) {

	from, size = plot.checkParams(from, size)

	metadatas, total, gerr := plot.persist.metaStorage.FilterMetadata(keySet, plot.toMetaParam(metric, tsType, tags), from, size)

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
