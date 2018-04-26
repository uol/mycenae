package plot

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
)

const DEFAULT_SIZE = 50

func (plot Plot) validateKeySet(keyset string) gobol.Error {

	found, gerr := plot.persist.metaStorage.CheckKeySet(keyset)
	if gerr != nil {
		return gerr
	}
	if !found {
		return errNotFound("ListTags")
	}

	return nil
}

func (plot Plot) checkParams(from, size int) (int, int) {

	if from < 0 {
		from = 0
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return from, size
}

func (plot Plot) FilterMetrics(keyset, metricName string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeySet(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterMetrics")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterMetrics(keyset, metricName, size)
}

func (plot Plot) FilterTagKeys(keyset, tagKname string, size int) ([]string, int, gobol.Error) {

	err := plot.validateKeySet(keyset)
	if err != nil {
		return nil, 0, errNotFound("FilterTagKeys")
	}

	if size <= 0 {
		size = plot.defaultMaxResults
	}

	return plot.persist.metaStorage.FilterTagKeys(keyset, tagKname, size)
}

func (plot Plot) FilterTagValues(keyset, tagVname string, size int) ([]string, int, gobol.Error) {

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
func (plot Plot) toMetaParam(metric string, tags map[string]string) *metadata.Metadata {

	metaParams := &metadata.Metadata{Metric: metric}

	if len(tags) > 0 {
		keys := []string{}
		values := []string{}
		for k, v := range tags {
			keys = append(keys, k)
			values = append(values, v)
		}
		metaParams.TagKey = keys
		metaParams.TagValue = values
	}

	return metaParams
}

// toMetaParam - converts metric and tags to a Metadata struct to be used as query
func (plot Plot) toMetaParamArray(metric string, tags map[string][]string) *metadata.Metadata {

	metaParams := &metadata.Metadata{Metric: metric}

	if len(tags) > 0 {
		keys := []string{}
		values := []string{}
		for k, v := range tags {
			keys = append(keys, k)
			values = append(values, v...)
		}
	}

	return metaParams
}

// extractTagMap - extracts all tags and tag values to
func (plot Plot) extractTagMap(metadata *metadata.Metadata) map[string]string {

	tagMap := map[string]string{}
	for i := 0; i < len(metadata.TagKey); i++ {
		tagMap[metadata.TagKey[i]] = metadata.TagValue[i]
	}

	return tagMap
}

func (plot Plot) ListMeta(keySet, tsType, metric string, tags map[string]string, onlyids bool, size, from int) ([]TsMetaInfo, int, gobol.Error) {

	from, size = plot.checkParams(from, size)

	metadatas, total, gerr := plot.persist.metaStorage.ListMetadata(keySet, tsType, plot.toMetaParam(metric, tags), from, size)

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
