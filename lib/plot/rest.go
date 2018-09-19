package plot

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/structs"
)

func (plot *Plot) ListPoints(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/points", "keyset": "empty"})
		rip.Fail(w, errNotFound("ListPoints"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/points", "keyset": keyset})

	query := structs.TsQuery{}

	err := rip.FromJSON(r, &query)
	if err != nil {
		rip.Fail(w, err)
		return
	}

	mts := make(map[string]*Series)

	empty := 0

	for _, k := range query.Keys {

		key := []string{k.TSid}

		if k.TTL == 0 {
			k.TTL = plot.defaultTTL
		}

		opers := structs.DataOperations{
			Downsample: query.Downsample,
			Order: []string{
				"downsample",
				"aggregation",
				"rate",
			},
		}

		sPoints, gerr := plot.GetTimeSeries(
			k.TTL,
			key,
			query.Start,
			query.End,
			opers,
			true,
			true,
		)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if sPoints.Count == 0 {
			empty++
		}

		var returnSerie [][]interface{}

		for _, point := range sPoints.Data {

			var pointArray []interface{}

			pointArray = append(pointArray, point.Date)

			if point.Empty {
				pointArray = append(pointArray, nil)
			} else {
				pointArray = append(pointArray, point.Value)
			}

			returnSerie = append(returnSerie, pointArray)

		}

		s := SeriesType{
			Count: sPoints.Count,
			Total: sPoints.Total,
			Ts:    returnSerie,
		}

		series := new(Series)

		series.Points = s

		mts[k.TSid] = series

	}

	for _, k := range query.Text {

		key := []string{k.TSid}

		if k.TTL == 0 {
			k.TTL = plot.defaultTTL
		}

		sPoints, gerr := plot.GetTextSeries(
			k.TTL,
			key,
			query.Start,
			query.End,
			"",
			true,
			query.GetRe(),
			query.Downsample,
		)

		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if sPoints.Count == 0 {
			empty++
		}

		var returnSerie [][]interface{}

		for _, point := range sPoints.Data {

			var pointArray []interface{}

			pointArray = append(pointArray, point.Date)

			pointArray = append(pointArray, point.Value)

			returnSerie = append(returnSerie, pointArray)

		}

		s := SeriesType{
			Count: sPoints.Count,
			Total: sPoints.Total,
			Ts:    returnSerie,
		}

		series := new(Series)

		series.Text = s

		mts[k.TSid] = series

	}

	if len(query.Merge) > 0 {

		for name, ks := range query.Merge {

			var ids []string

			series := new(Series)

			for _, k := range ks.Keys {

				ids = append(ids, k.TSid)

			}

			sPoints := SeriesType{}

			if ks.Keys[0].TSid[:1] == "T" {

				if ks.Keys[0].TTL == 0 {
					ks.Keys[0].TTL = plot.defaultTTL
				}

				serie, gerr := plot.GetTextSeries(
					ks.Keys[0].TTL,
					ids,
					query.Start,
					query.End,
					ks.Option,
					true,
					query.GetRe(),
					query.Downsample,
				)
				if gerr != nil {
					rip.Fail(w, gerr)
					return
				}

				var returnSerie [][]interface{}

				for _, point := range serie.Data {

					var pointArray []interface{}

					pointArray = append(pointArray, point.Date)

					pointArray = append(pointArray, point.Value)

					returnSerie = append(returnSerie, pointArray)
				}

				sPoints = SeriesType{
					Count: serie.Count,
					Total: serie.Total,
					Ts:    returnSerie,
				}

			} else {

				opers := structs.DataOperations{
					Downsample: query.Downsample,
					Merge:      ks.Option,
					Order: []string{
						"downsample",
						"aggregation",
						"rate",
					},
				}

				if ks.Keys[0].TTL == 0 {
					ks.Keys[0].TTL = plot.defaultTTL
				}

				serie, gerr := plot.GetTimeSeries(
					ks.Keys[0].TTL,
					ids,
					query.Start,
					query.End,
					opers,
					true,
					true,
				)
				if gerr != nil {
					rip.Fail(w, gerr)
					return
				}

				var returnSerie [][]interface{}

				for _, point := range serie.Data {

					var pointArray []interface{}

					pointArray = append(pointArray, point.Date)

					if point.Empty {
						pointArray = append(pointArray, nil)
					} else {
						pointArray = append(pointArray, point.Value)
					}

					returnSerie = append(returnSerie, pointArray)

				}

				sPoints = SeriesType{
					Count: serie.Count,
					Total: serie.Total,
					Ts:    returnSerie,
				}

			}

			id := fmt.Sprintf("%v|merged:[%v]", keyset, name)

			series.Points = sPoints

			mts[id] = series

		}

	}

	if len(query.Keys)+len(query.Text)+len(query.Merge) == empty {
		gerr := errNoContent("ListPoints")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		Payload: mts,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

// getSizeParameter - return parameter 'size'
func (plot *Plot) getSizeParameter(w http.ResponseWriter, q url.Values, function string) (int, bool) {

	sizeStr := q.Get("size")
	var err error
	size := plot.defaultMaxResults

	if sizeStr != "" {
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			rip.Fail(w, errParamSize(function, err))
			return size, true
		}
		if size <= 0 {
			rip.Fail(w, errParamSize(function, errors.New("")))
			return size, true
		}
	}

	return size, false
}

func (plot *Plot) ListTagsNumber(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "number", map[string]string{"path": "/keysets/#keyset/tags"})
}

func (plot *Plot) ListTagsText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "text", map[string]string{"path": "/keysets/#keyset/text/tags"})
}

func (plot *Plot) listTags(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string, smap map[string]string) {

	keyset := ps.ByName("keyset")
	if keyset == "" {
		smap["keyset"] = "empty"
		rip.AddStatsMap(r, smap)
		rip.Fail(w, errNotFound("listTags"))
		return
	}

	smap["keyset"] = keyset
	rip.AddStatsMap(r, smap)

	q := r.URL.Query()

	size, fail := plot.getSizeParameter(w, q, "listTags")
	if fail {
		return
	}

	tags, total, gerr := plot.FilterTagKeys(keyset, q.Get("tag"), size)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(tags) == 0 {
		gerr := errNoContent("ListTags")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      tags,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

func (plot *Plot) ListMetricsNumber(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMetrics(w, r, ps, "metric", map[string]string{"path": "/keysets/#keyset/metrics"})
}

func (plot *Plot) ListMetricsText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMetrics(w, r, ps, "metrictext", map[string]string{"path": "/keysets/#keyset/text/metrics"})
}

func (plot *Plot) listMetrics(w http.ResponseWriter, r *http.Request, ps httprouter.Params, esType string, smap map[string]string) {

	keyset := ps.ByName("keyset")
	if keyset == "" {
		smap["keyset"] = "empty"
		rip.AddStatsMap(r, smap)
		rip.Fail(w, errNotFound("listMetrics"))
		return
	}

	smap["keyset"] = keyset
	rip.AddStatsMap(r, smap)

	q := r.URL.Query()

	size, fail := plot.getSizeParameter(w, q, "ListMetrics")
	if fail {
		return
	}

	metrics, total, gerr := plot.FilterMetrics(keyset, q.Get("metric"), size)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(metrics) == 0 {
		gerr := errNoContent("ListMetrics")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      metrics,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

func (plot *Plot) ListMetaNumber(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMeta(w, r, ps, "meta", map[string]string{"path": "/keysets/#keyset/meta"})
}

func (plot *Plot) ListMetaText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMeta(w, r, ps, "metatext", map[string]string{"path": "/keysets/#keyset/text/meta"})
}

// getKeysetParameter - returns the keyset parameter
func (plot *Plot) getKeysetParameter(w http.ResponseWriter, r *http.Request, ps httprouter.Params, functionName string, smap map[string]string) (*string, bool) {

	keyset := ps.ByName("keyset")
	if keyset == "" {
		smap["keyset"] = "empty"
		rip.AddStatsMap(r, smap)
		err := errNotFound(functionName)
		rip.Fail(w, err)
		return nil, true
	}

	return &keyset, false
}

// getQueryParameter - extracts all query parameters found in the request
func (plot *Plot) getQueryParameter(w http.ResponseWriter, r *http.Request, ps httprouter.Params, functionName string, smap map[string]string) (*TSmeta, *string, bool) {

	keyset, fail := plot.getKeysetParameter(w, r, ps, functionName, smap)
	if fail {
		return nil, nil, true
	}

	smap["keyset"] = *keyset
	rip.AddStatsMap(r, smap)

	err := plot.validateKeySet(*keyset)
	if err != nil {
		err := errNotFound(functionName)
		rip.Fail(w, err)
		return nil, keyset, true
	}

	query := TSmeta{}

	gerr := rip.FromJSON(r, &query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return nil, keyset, true
	}

	return &query, keyset, false
}

// getFromParameter - returns the "from" parameter
func (plot *Plot) getFromParameter(w http.ResponseWriter, q url.Values, function string) (int, bool) {

	fromStr := q.Get("from")
	from := 0

	if fromStr != "" {
		var err error
		from, err = strconv.Atoi(fromStr)
		if err != nil {
			rip.Fail(w, errParamFrom(function, err))
			return 0, true
		}
	}

	return from, false
}

func (plot *Plot) listMeta(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string, smap map[string]string) {

	query, keyset, fail := plot.getQueryParameter(w, r, ps, "listMeta", smap)
	if fail {
		return
	}

	q := r.URL.Query()

	size, fail := plot.getSizeParameter(w, q, "ListMeta")
	if fail {
		return
	}

	from, fail := plot.getFromParameter(w, q, "ListMeta")
	if fail {
		return
	}

	onlyidsStr := q.Get("onlyids")

	var onlyids bool

	if onlyidsStr != "" {
		var err error
		onlyids, err = strconv.ParseBool(onlyidsStr)
		if err != nil {
			gerr := errValidation("ListMeta", `query param "onlyids" should be a boolean`, err)
			rip.Fail(w, gerr)
			return
		}
	}

	tags := map[string]string{}

	for _, tag := range query.Tags {
		tags[tag.Key] = tag.Value
	}

	keys, total, gerr := plot.ListMeta(*keyset, tsType, query.Metric, tags, onlyids, size, from)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(keys) == 0 {
		gerr := errNoContent("ListMeta")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      keys,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

// DeleteNumberTS - delete number serie(s)
func (plot *Plot) DeleteNumberTS(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.deleteTS(w, r, ps, "meta", map[string]string{"path": "/keysets/#keyset/delete/meta"})
}

// DeleteTextTS - delete text serie(s)
func (plot *Plot) DeleteTextTS(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.deleteTS(w, r, ps, "metatext", map[string]string{"path": "/keysets/#keyset/delete/text/meta"})
}

func (plot *Plot) deleteTS(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string, smap map[string]string) {

	query, keyset, fail := plot.getQueryParameter(w, r, ps, "deleteTS", smap)
	if fail {
		return
	}

	tags := map[string]string{}

	for _, tag := range query.Tags {
		tags[tag.Key] = tag.Value
	}

	keys, total, gerr := plot.ListMeta(*keyset, tsType, query.Metric, tags, false, plot.defaultMaxResults, 0)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(keys) == 0 {
		gerr := errNoContent("deleteTS")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      keys,
	}

	q := r.URL.Query()
	commit := q.Get("commit")

	if commit != "true" {

		rip.SuccessJSON(w, http.StatusOK, out)

	} else {

		for _, key := range keys {
			gerr = plot.persist.metaStorage.DeleteDocumentByID(*keyset, tsType, key.TsId)
			if gerr != nil {
				rip.Fail(w, gerr)
				return
			}
		}

		rip.SuccessJSON(w, http.StatusAccepted, out)
		return
	}
}

func (plot *Plot) ListNumberMetaTagValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMetaTagValues(w, r, ps, "meta", map[string]string{"path": "/keysets/#keyset/tag/values"})
}

func (plot *Plot) ListTextMetaTagValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMetaTagValues(w, r, ps, "metatext", map[string]string{"path": "/keysets/#keyset/text/tag/values"})
}

// listMetaTagValues
func (plot *Plot) listMetaTagValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string, smap map[string]string) {

	functionName := "listMetaValues"
	keyset, fail := plot.getKeysetParameter(w, r, ps, functionName, smap)
	if fail {
		return
	}

	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		rip.Fail(w, errMandatoryParam(functionName, "metric"))
		return
	}

	tag := q.Get("tag")
	if tag == "" {
		rip.Fail(w, errMandatoryParam(functionName, "tag"))
		return
	}

	value := q.Get("value")
	if value == "" {
		value = "*"
	}

	size, fail := plot.getSizeParameter(w, q, functionName)
	if fail {
		return
	}

	keys, total, gerr := plot.persist.metaStorage.FilterTagValuesByMetricAndTag(*keyset, tsType, metric, tag, value, size)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(keys) == 0 {
		gerr := errNoContent("ListMeta")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      keys,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}
