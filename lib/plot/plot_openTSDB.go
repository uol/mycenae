package plot

import (
	"fmt"
	"strings"

	"github.com/uol/gobol"

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

	metadatas, total, gerr := plot.persist.metaStorage.ListMetadata(keyset, "meta", plot.toMetaParamArray(metric, tags), from, size)

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

// SplitTagValues - splits the query if '|' is found
func (plot *Plot) splitTagValues(value string) string {

	if strings.Contains(value, "|") {
		q := "("
		values := strings.Split(value, "|")
		size := len(values)
		for i := 0; i < size; i++ {
			q += fmt.Sprintf("tagValue:%s", values[i])
			if i < size-1 {
				q += " OR "
			}
		}
		q += ")"

		return q
	}

	return fmt.Sprintf("tagValue:%s", value)
}

func (plot *Plot) MetaFilterOpenTSDB(keyset, metric string, filters []structs.TSDBfilter, size int) ([]TSDBobj, int, gobol.Error) {

	from, size := plot.checkParams(0, size)

	q := fmt.Sprintf("metric:%s AND type:meta", metric)

	for _, filter := range filters {

		if filter.Ftype == "regexp" {
			filter.Filter = plot.persist.metaStorage.SetRegexValue(filter.Filter)
		}

		if filter.Ftype == "not_literal_or" {
			q += fmt.Sprintf(" AND -(tagKey:%s AND %s)", filter.Tagk, plot.splitTagValues(filter.Filter))
		} else {
			q += fmt.Sprintf(" AND tagKey:%s AND %s", filter.Tagk, plot.splitTagValues(filter.Filter))
		}
	}

	fmt.Println(q)

	metadatas, total, gerr := plot.persist.metaStorage.Query(keyset, q, from, size)

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
