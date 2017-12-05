package plot

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/structs"
)

func (plot *Plot) ListPoints(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/points", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ListPoints"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/points", "keyspace": keyspace})


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

		opers := structs.DataOperations{
			Downsample: query.Downsample,
			Order: []string{
				"downsample",
				"aggregation",
				"rate",
			},
		}

		sPoints, gerr := plot.GetTimeSeries(
			keyspace,
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

		sPoints, gerr := plot.GetTextSeries(
			keyspace,
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
				serie, gerr := plot.GetTextSeries(
					keyspace,
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

				serie, gerr := plot.GetTimeSeries(
					keyspace,
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

			id := fmt.Sprintf("%v|merged:[%v]", keyspace, name)

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

func (plot *Plot) ListTagsNumber(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "tagk", map[string]string{"path": "/keyspaces/#keyspace/tags"})
}

func (plot *Plot) ListTagsText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "tagktext", map[string]string{"path": "keyspaces/#keyspace/text/tags"})
}

func (plot *Plot) listTags(w http.ResponseWriter, r *http.Request, ps httprouter.Params, esType string, smap map[string]string) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		smap["keyspace"] = "empty"
		rip.AddStatsMap(r, smap)
		rip.Fail(w, errNotFound("listTags"))
		return
	}

	smap["keyspace"] = keyspace
	rip.AddStatsMap(r, smap)

	q := r.URL.Query()

	sizeStr := q.Get("size")

	var size int
	var err error

	if sizeStr != "" {
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			gerr := errParamSize("ListTags", err)
			rip.Fail(w, gerr)
			return
		}

		if size <= 0 {
			gerr := errParamSize("ListTags", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	fromStr := q.Get("from")

	var from int

	if fromStr != "" {
		from, err = strconv.Atoi(fromStr)
		if err != nil {
			gerr := errParamFrom("ListTags", err)
			rip.Fail(w, gerr)
			return
		}
		if from < 0 {
			gerr := errParamFrom("ListTags", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	tags, total, gerr := plot.ListTags(keyspace, esType, q.Get("tag"), int64(size), int64(from))
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
	plot.listMetrics(w, r, ps, "metric", map[string]string{"path": "/keyspaces/#keyspace/metrics"})
}

func (plot *Plot) ListMetricsText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMetrics(w, r, ps, "metrictext", map[string]string{"path": "keyspaces/#keyspace/text/metrics"})
}

func (plot *Plot) listMetrics(w http.ResponseWriter, r *http.Request, ps httprouter.Params, esType string, smap map[string]string) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		smap["keyspace"] = "empty"
		rip.AddStatsMap(r, smap)
		rip.Fail(w, errNotFound("listMetrics"))
		return
	}

	smap["keyspace"] = keyspace
	rip.AddStatsMap(r, smap)

	q := r.URL.Query()
	sizeStr := q.Get("size")

	var size int
	var err error

	if sizeStr != "" {
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			gerr := errParamSize("ListMetrics", err)
			rip.Fail(w, gerr)
			return
		}
		if size <= 0 {
			gerr := errParamSize("ListMetrics", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	fromStr := q.Get("from")

	var from int

	if fromStr != "" {
		from, err = strconv.Atoi(fromStr)
		if err != nil {
			gerr := errParamFrom("ListMetrics", err)
			rip.Fail(w, gerr)
			return
		}
		if from < 0 {
			gerr := errParamFrom("ListMetrics", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	metrics, total, gerr := plot.ListMetrics(keyspace, esType, q.Get("metric"), int64(size), int64(from))
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
	plot.listMeta(w, r, ps, "meta", map[string]string{"path": "/keyspaces/#keyspace/meta"})
}

func (plot *Plot) ListMetaText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMeta(w, r, ps, "metatext", map[string]string{"path": "keyspaces/#keyspace/text/meta"})
}

func (plot *Plot) listMeta(w http.ResponseWriter, r *http.Request, ps httprouter.Params, esType string, smap map[string]string) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		smap["keyspace"] = "empty"
		rip.AddStatsMap(r, smap)
		rip.Fail(w, errNotFound("listMeta"))
		return
	}

	smap["keyspace"] = keyspace
	rip.AddStatsMap(r, smap)

	q := r.URL.Query()
	query := TSmeta{}

	gerr := rip.FromJSON(r, &query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	sizeStr := q.Get("size")

	var size int
	var err error

	if sizeStr != "" {
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			rip.Fail(w, errParamSize("ListMeta", err))
			return
		}
		if size <= 0 {
			rip.Fail(w, errParamSize("ListMeta", errors.New("")))
			return
		}
	}

	fromStr := q.Get("from")

	var from int

	if fromStr != "" {
		from, err = strconv.Atoi(fromStr)
		if err != nil {
			gerr := errParamFrom("ListMeta", err)
			rip.Fail(w, gerr)
			return
		}
		if from < 0 {
			gerr := errParamFrom("ListMeta", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	onlyidsStr := q.Get("onlyids")

	var onlyids bool

	if onlyidsStr != "" {
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

	keys, total, gerr := plot.ListMeta(keyspace, esType, query.Metric, tags, onlyids, int64(size), int64(from))
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
