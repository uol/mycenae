package plot

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/parser"
	"github.com/uol/mycenae/lib/structs"
)

func (plot *Plot) Lookup(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/search/lookup", constants.StringsKeyset: "empty"})
		rip.Fail(w, errNotFound("Lookup"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/search/lookup", constants.StringsKeyset: keyset})

	m := r.URL.Query().Get("m")

	if m == constants.StringsEmpty {
		gerr := errValidationS("Lookup", `missing query parameter "m"`)
		rip.Fail(w, gerr)
		return
	}

	metric, tags, gerr := parseQuery(m)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	tagMap := map[string][]string{}
	for _, tag := range tags {
		if _, ok := tagMap[tag.Key]; !ok {
			tagMap[tag.Key] = []string{}
		}
		tagMap[tag.Key] = append(tagMap[tag.Key], tag.Value)
	}

	tsds, total, gerr := plot.MetaOpenTSDB(keyset, metric, tagMap, plot.MaxTimeseries, 0)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	logIfExceeded := fmt.Sprintf("TS THRESHOLD/MAX EXCEEDED: %+v", m)
	gerr = plot.checkTotalTSLimits(logIfExceeded, keyset, metric, total)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	look := TSDBlookup{
		Type:         "LOOKUP",
		Metric:       metric,
		Tags:         tags,
		Results:      tsds,
		TotalResults: total,
	}

	rip.SuccessJSON(w, http.StatusOK, look)
	return
}

func (plot *Plot) Suggest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/suggest", constants.StringsKeyset: "empty"})
		rip.Fail(w, errNotFound("Suggest"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/suggest", constants.StringsKeyset: keyset})

	queryString := r.URL.Query()

	maxStr := queryString.Get("max")
	max := plot.defaultMaxResults
	resp := []string{}

	var err error

	if maxStr != constants.StringsEmpty {

		max, err = strconv.Atoi(maxStr)
		if err != nil {
			gerr := errValidationE("Suggest", err)
			rip.Fail(w, gerr)
			return
		}
	}

	var gerr gobol.Error

	switch queryString.Get("type") {
	case constants.StringsEmpty:
		gerr = errValidationS("Suggest", "type required")
		rip.Fail(w, gerr)
		return
	case "metrics":
		q := fmt.Sprintf("%v*", queryString.Get("q"))
		resp, _, gerr = plot.FilterMetrics(keyset, q, max)
	case "tagk":
		q := fmt.Sprintf("%v*", queryString.Get("q"))
		resp, _, gerr = plot.FilterTagKeys(keyset, q, max)
	case "tagv":
		q := fmt.Sprintf("%v*", queryString.Get("q"))
		resp, _, gerr = plot.FilterTagValues(keyset, q, max)
	default:
		gerr = errValidationS("Suggest", "unsupported type")
		rip.Fail(w, gerr)
		return
	}
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	sort.Strings(resp)

	rip.SuccessJSON(w, http.StatusOK, resp)
	return
}

func (plot *Plot) Query(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/query", constants.StringsKeyset: "empty"})
		rip.Fail(w, errNotFound("Query"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/query", constants.StringsKeyset: keyset})

	query := structs.TSDBqueryPayload{}

	gerr := rip.FromJSON(r, &query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	resps, numBytes, gerr := plot.getTimeseries(keyset, query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	addProcessedBytesHeader(w, numBytes)

	if !query.EstimateSize {

		if len(resps) == 0 {
			rip.SuccessJSON(w, http.StatusOK, []string{})
			return
		}

		rip.SuccessJSON(w, http.StatusOK, resps)
		return
	}

	rip.Success(w, http.StatusOK, []byte(fmt.Sprintf("%d bytes", numBytes)))
	return
}

func (plot *Plot) getTimeseries(
	keyset string,
	query structs.TSDBqueryPayload,
) (resps TSDBresponses, sumBytes uint32, gerr gobol.Error) {

	if query.Relative != constants.StringsEmpty {
		now := time.Now()
		start, gerr := parser.GetRelativeStart(now, query.Relative)
		if gerr != nil {
			return resps, 0, gerr
		}
		query.Start = start.UnixNano() / 1e+6
		query.End = now.UnixNano() / 1e+6
	} else {
		if query.Start == 0 {
			return resps, 0, errValidationS("getTimeseries", "start cannot be zero")
		}

		if query.End == 0 {
			query.End = time.Now().UnixNano() / 1e+6
		}

		if query.End < query.Start {
			return resps, 0, errValidationS("getTimeseries", "end date should be equal or bigger than start date")
		}
	}

	oldDs := structs.Downsample{}

	sumTotalPoints := 0
	sumCountPoints := 0

	for _, q := range query.Queries {

		if q.Downsample != constants.StringsEmpty {

			ds := strings.Split(q.Downsample, "-")
			var unit string
			var val int

			if string(ds[0][len(ds[0])-2:]) == "ms" {
				unit = ds[0][len(ds[0])-2:]
				val, _ = strconv.Atoi(ds[0][:len(ds[0])-2])
			} else {
				unit = ds[0][len(ds[0])-1:]
				val, _ = strconv.Atoi(ds[0][:len(ds[0])-1])
			}

			apporx := ds[1]

			if apporx == "count" {
				apporx = "pnt"
			}

			switch unit {
			case "ms":
				oldDs.Options.Unit = "ms"
			case "s":
				oldDs.Options.Unit = "sec"
			case "m":
				oldDs.Options.Unit = "min"
			case "h":
				oldDs.Options.Unit = "hour"
			case "d":
				oldDs.Options.Unit = "day"
			case "w":
				oldDs.Options.Unit = "week"
			case "n":
				oldDs.Options.Unit = "month"
			case "y":
				oldDs.Options.Unit = "year"
			}

			if len(ds) == 3 {
				oldDs.Options.Fill = ds[2]
			} else {
				oldDs.Options.Fill = "none"
			}

			oldDs.Options.Downsample = apporx
			oldDs.Options.Value = val
			oldDs.Enabled = true

		}

		for k, v := range q.Tags {

			members := strings.Split(v, "|")
			filter := structs.TSDBfilter{
				Ftype:   "wildcard",
				Tagk:    k,
				Filter:  v,
				GroupBy: members[0] == "*" || len(members) > 1,
			}

			q.Filters = append(q.Filters, filter)
		}

		tagMap := map[string][]string{}
		ttl := plot.defaultTTL
		ttlIndex := -1

		for i, filter := range q.Filters {
			if _, ok := tagMap[filter.Tagk]; ok {
				tagMap[filter.Tagk] = append(tagMap[filter.Tagk], filter.Filter)
			} else {
				if filter.Tagk == "ttl" {
					v, err := strconv.Atoi(filter.Filter)
					if err != nil {
						return resps, sumBytes, errValidationE("getTimeseries", err)
					}
					ttl = v
					ttlIndex = i
				}
				tagMap[filter.Tagk] = []string{filter.Filter}
			}
		}

		if ttlIndex >= 0 {
			q.Filters = append(q.Filters[:ttlIndex], q.Filters[ttlIndex+1:]...)
		}

		tsobs, total, gerr := plot.MetaFilterOpenTSDB(keyset, q.Metric, q.Filters, plot.MaxTimeseries)
		if gerr != nil {
			return resps, sumBytes, gerr
		}

		logIfExceeded := fmt.Sprintf("TS THRESHOLD/MAX EXCEEDED for query: %+v", query)
		gerr = plot.checkTotalTSLimits(logIfExceeded, keyset, q.Metric, total)
		if gerr != nil {
			return TSDBresponses{}, sumBytes, gerr
		}

		if len(tsobs) == 0 {
			continue
		}

		groups := plot.GetGroups(q.Filters, tsobs)

		for _, group := range groups {
			ids := []string{}
			tagK := make(map[string]map[string]string)

			for _, tsd := range group {

				for k, v := range tsd.Tags {
					if _, ok := tagK[k]; ok {
						tagK[k][v] = constants.StringsEmpty
					} else {
						tagK[k] = map[string]string{
							v: constants.StringsEmpty,
						}
					}
				}

				ids = append(ids, tsd.Tsuid)

			}

			aggTags := []string{}

			if q.RateOptions.CounterMax == nil {
				var maxInt int64
				maxInt = 1<<63 - 1
				q.RateOptions.CounterMax = &maxInt
			}

			filterV := structs.FilterValueOperation{}

			if q.FilterValue != constants.StringsEmpty {
				filterV.Enabled = true
				if q.FilterValue[:2] == ">=" || q.FilterValue[:2] == "<=" || q.FilterValue[:2] == "==" || q.FilterValue[:2] == "!=" {
					val, err := strconv.ParseFloat(q.FilterValue[2:], 64)
					if err != nil {
						return resps, sumBytes, errValidationE("getTimeseries", err)
					}
					filterV.BoolOper = q.FilterValue[:2]
					filterV.Value = val
				} else if q.FilterValue[:1] == ">" || q.FilterValue[:1] == "<" {
					val, err := strconv.ParseFloat(q.FilterValue[1:], 64)
					if err != nil {
						return resps, sumBytes, errValidationE("getTimeseries", err)
					}
					filterV.BoolOper = q.FilterValue[:1]
					filterV.Value = val
				}
			}

			merge := q.Aggregator

			if q.Aggregator == "count" {
				merge = "pnt"
			}

			opers := structs.DataOperations{
				Downsample: oldDs,
				Merge:      merge,
				Rate: structs.RateOperation{
					Enabled: q.Rate,
					Options: q.RateOptions,
				},
				FilterValue: filterV,
				Order:       q.Order,
			}

			keepEmpty := false

			if oldDs.Options.Fill != "none" {
				keepEmpty = true
			}

			serie, numBytes, gerr := plot.GetTimeSeries(
				ttl,
				ids,
				query.Start,
				query.End,
				opers,
				query.MsResolution,
				keepEmpty,
				query.EstimateSize,
				keyset,
			)
			if gerr != nil {
				if gerr.Error() == plot.persist.maxBytesErr.Error() {
					return resps, sumBytes, errMaxBytesLimit("getTimeseries", keyset, q.Metric, query.Start, query.End, ttl)
				}

				return resps, sumBytes, gerr
			}

			sumTotalPoints += serie.Total
			sumCountPoints += serie.Count
			sumBytes += numBytes

			for k, kv := range tagK {
				if len(kv) > 1 {
					aggTags = append(aggTags, k)
				}
			}

			sort.Strings(aggTags)

			points := map[string]interface{}{}

			for _, point := range serie.Data {

				k := point.Date

				if !query.MsResolution {
					k = point.Date / 1000
				}

				ksrt := strconv.FormatInt(k, 10)
				if point.Empty {
					switch oldDs.Options.Fill {
					case "null":
						points[ksrt] = nil
					case "nan":
						points[ksrt] = "NaN"
					default:
						points[ksrt] = point.Value
					}
				} else {
					points[ksrt] = point.Value
				}

			}

			if len(points) > 0 {
				tagsU := make(map[string]string)

				for k, kv := range tagK {
					if len(kv) == 1 {
						for v := range kv {
							tagsU[k] = v
						}
					}
				}

				resp := TSDBresponse{
					Metric:         q.Metric,
					Tags:           tagsU,
					AggregatedTags: aggTags,
					Dps:            points,
				}

				if query.ShowTSUIDs {
					resp.Tsuids = ids
				}

				resps = append(resps, resp)
			}

		}

		plot.statsConferMetric(keyset, q.Metric)
	}

	plot.statsPlotSummaryPoints(sumCountPoints, sumTotalPoints, sumBytes, keyset)

	sort.Sort(resps)

	return resps, sumBytes, gerr
}

func parseQuery(query string) (string, []Tag, gobol.Error) {

	metric, sub := getMetric(query)

	tags, gerr := getTags(sub)

	return metric, tags, gerr
}

func getMetric(query string) (string, string) {

	mr := []rune{}

	for i, r := range query {
		if string(r) == "{" {
			return strings.TrimSpace(string(mr)), query[i:]
		}
		mr = append(mr, r)
	}

	return strings.TrimSpace(string(mr)), constants.StringsEmpty
}

func getTags(query string) ([]Tag, gobol.Error) {

	if len(query) == 0 {
		return []Tag{}, nil
	}

	if string(query[0]) != "{" || string(query[len(query)-1]) != "}" {
		return []Tag{}, errValidationS("getTags", `Missing '}' at the end of query`)
	}

	query = query[1:]

	mr := []rune{}

	var key string

	tags := []Tag{}

	for _, r := range query {

		if string(r) == "=" {
			key = strings.TrimSpace(string(mr))
			mr = []rune{}
			continue
		}

		if string(r) == "," || string(r) == "}" {
			if key == constants.StringsEmpty {
				return []Tag{}, errValidationS("getTags", `invalid tag format`)
			}

			tag := Tag{
				Key:   key,
				Value: strings.TrimSpace(string(mr)),
			}

			tags = append(tags, tag)
			mr = []rune{}
			continue
		}

		mr = append(mr, r)
	}

	return tags, nil
}
