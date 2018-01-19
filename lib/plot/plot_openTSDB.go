package plot

import (
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

func (plot *Plot) MetaOpenTSDB(
	keyset,
	id,
	metric string,
	tags map[string][]string,
	size,
	from int64,
) ([]TSDBobj, int, gobol.Error) {

	esType := "meta"

	var esQuery QueryWrapper

	if metric != "" && metric != "*" {

		metricTerm := Term{
			Term: map[string]string{
				"metric": metric,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, metricTerm)
	}

	for k, vs := range tags {

		var esQueryNest EsNestedQuery

		esQueryNest.Nested.Path = "tagsNested"

		for _, v := range vs {

			tagKTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsNested.tagKey": k,
				},
			}

			if v == "*" {
				v = ".*"
			}

			tagVTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsNested.tagValue": v,
				},
			}

			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, esQueryNest)

	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 10000
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseMeta

	gerr := plot.persist.ListESMeta(keyset, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tsds []TSDBobj

	for _, docs := range esResp.Hits.Hits {

		mapTags := map[string]string{}

		for _, tag := range docs.Source.Tags {
			mapTags[tag.Key] = tag.Value
		}

		tsd := TSDBobj{
			Tsuid:  docs.Source.ID,
			Metric: docs.Source.Metric,
			Tags:   mapTags,
		}

		tsds = append(tsds, tsd)
	}

	return tsds, total, gerr
}

func (plot *Plot) MetaFilterOpenTSDB(
	keyset,
	id,
	metric string,
	filters []structs.TSDBfilter,
	size int64,
) ([]TSDBobj, int, gobol.Error) {

	esType := "meta"

	esQuery := QueryWrapper{
		Size: size,
	}

	if metric != "" && metric != "*" {

		metricTerm := Term{
			Term: map[string]string{
				"metric": metric,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, metricTerm)
	}

	for _, filter := range filters {

		var esQueryNest EsNestedQuery

		esQueryNest.Nested.Path = "tagsNested"

		tk := filter.Tagk

		tk = strings.Replace(tk, ".", "\\.", -1)
		tk = strings.Replace(tk, "&", "\\&", -1)
		tk = strings.Replace(tk, "#", "\\#", -1)

		tagKTerm := EsRegexp{
			Regexp: map[string]string{
				"tagsNested.tagKey": tk,
			},
		}

		v := filter.Filter

		if filter.Ftype != "regexp" {
			v = strings.Replace(v, ".", "\\.", -1)
			v = strings.Replace(v, "&", "\\&", -1)
			v = strings.Replace(v, "#", "\\#", -1)
		}

		if filter.Ftype == "wildcard" {
			v = strings.Replace(v, "*", ".*", -1)
		}

		tagVTerm := EsRegexp{
			Regexp: map[string]string{
				"tagsNested.tagValue": v,
			},
		}

		esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)

		if filter.Ftype == "not_literal_or" {
			esQueryNest.Nested.Query.Bool.MustNot = append(esQueryNest.Nested.Query.Bool.MustNot, tagVTerm)
		} else {
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, esQueryNest)

	}

	var esResp EsResponseMeta

	gerr := plot.persist.ListESMeta(keyset, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tsds []TSDBobj

	for _, docs := range esResp.Hits.Hits {

		mapTags := map[string]string{}

		for _, tag := range docs.Source.Tags {
			mapTags[tag.Key] = tag.Value
		}

		tsd := TSDBobj{
			Tsuid:  docs.Source.ID,
			Metric: docs.Source.Metric,
			Tags:   mapTags,
		}

		tsds = append(tsds, tsd)
	}

	return tsds, total, gerr
}
