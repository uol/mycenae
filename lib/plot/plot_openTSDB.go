package plot

import (
	"strings"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/structs"
)

func (plot Plot) GetGroups(filters []structs.TSDBfilter, tsobs []TSDBobj) (groups [][]TSDBobj) {

	if len(tsobs) == 0 {
		return groups
	}

	groups = append(groups, []TSDBobj{tsobs[0]})
	tsobs = append(tsobs[:0], tsobs[1:]...)
	deleted := 0

	for i := range tsobs {

		in := true

		j := i - deleted

		for k, group := range groups {

			in = true

			for _, filter := range filters {

				if !filter.GroupBy {
					continue
				}

				if group[0].Tags[filter.Tagk] != tsobs[0].Tags[filter.Tagk] {
					in = false
				}
			}

			if in {
				groups[k] = append(groups[k], tsobs[0])
				tsobs = append(tsobs[:j], tsobs[j+1:]...)
				deleted++
				break
			}

		}

		if !in {
			groups = append(groups, []TSDBobj{tsobs[0]})
			tsobs = append(tsobs[:j], tsobs[j+1:]...)
			deleted++
		}

	}

	return groups
}

func (plot *Plot) MetaOpenTSDB(keyset, metric string, tags map[string][]string, size, from int) ([]TSDBobj, int, gobol.Error) {

	from, size = plot.checkParams(from, size)

	metadatas, total, gerr := plot.persist.metaStorage.FilterMetadata(keyset, plot.toMetaParamArray(metric, "meta", tags), from, size)

	var tsds []TSDBobj

	for _, metadata := range metadatas {

		mapTags := plot.extractTagMap(&metadata)
		tsd := TSDBobj{
			Tsuid:  metadata.ID,
			Metric: metadata.Metric,
			Tags:   mapTags,
		}

		tsds = append(tsds, tsd)
	}

	return tsds, total, gerr
}

// SplitTagFilters - splits the query if '|' is found
func (plot *Plot) splitTagFilters(value string) []string {

	var values []string
	if strings.Contains(value, "|") {
		values = strings.Split(value, "|")
	} else {
		values = []string{value}
	}

	return values
}

// MetaFilterOpenTSDB - creates a metadata query
func (plot *Plot) MetaFilterOpenTSDB(keyset, metric string, filters []structs.TSDBfilter, size int) ([]TSDBobj, int, gobol.Error) {

	from, size := plot.checkParams(0, size)

	query := &metadata.Query{
		Metric:   metric,
		MetaType: "meta",
		Tags:     make([]metadata.QueryTag, len(filters)),
	}

	for i, filter := range filters {

		query.Tags[i] = metadata.QueryTag{
			Key:    filter.Tagk,
			Negate: filter.Ftype == "not_literal_or",
			Regexp: filter.Ftype == "regexp" || filter.Ftype == "wildcard",
		}

		if filter.Ftype != "not_literal_or" && (filter.Filter == "*" || filter.Filter == ".*") {
			continue
		}

		if filter.Ftype == "wildcard" {
			filter.Filter = strings.Replace(filter.Filter, ".", "\\.", -1)
			filter.Filter = strings.Replace(filter.Filter, "*", ".*", -1)
		}

		query.Tags[i].Values = plot.splitTagFilters(filter.Filter)
	}

	metadatas, total, gerr := plot.persist.metaStorage.FilterMetadata(keyset, query, from, size)

	var tsds []TSDBobj

	for _, metadata := range metadatas {

		mapTags := plot.extractTagMap(&metadata)

		tsd := TSDBobj{
			Tsuid:  metadata.ID,
			Metric: metadata.Metric,
			Tags:   mapTags,
		}

		tsds = append(tsds, tsd)
	}

	return tsds, total, gerr
}
