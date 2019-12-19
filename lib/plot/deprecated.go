package plot

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

// ListPoints - only used on unit tests... must be removed
func (plot *Plot) ListPoints(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/points", constants.StringsKeyset: "empty"})
		rip.Fail(w, errNotFound("ListPoints"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/points", constants.StringsKeyset: keyset})

	query := structs.TsQuery{}

	err := rip.FromJSON(r, &query)
	if err != nil {
		rip.Fail(w, err)
		return
	}

	mts := make(map[string]*Series)

	empty := 0

	var sumBytes uint32

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

		sPoints, numBytes, gerr := plot.GetTimeSeries(
			k.TTL,
			key,
			query.Start,
			query.End,
			opers,
			true,
			true,
			false,
			k.TSid,
		)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if sPoints.Count == 0 {
			empty++
		}

		sumBytes += numBytes

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

		sPoints, numBytes, gerr := plot.GetTextSeries(
			k.TTL,
			key,
			query.Start,
			query.End,
			query.GetRe(),
			k.TSid,
			false,
		)

		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if sPoints.Count == 0 {
			empty++
		}

		sumBytes += numBytes

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

				serie, numBytes, gerr := plot.GetTextSeries(
					ks.Keys[0].TTL,
					ids,
					query.Start,
					query.End,
					query.GetRe(),
					ks.Keys[0].TSid,
					false,
				)
				if gerr != nil {
					rip.Fail(w, gerr)
					return
				}

				sumBytes += numBytes

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

				serie, numBytes, gerr := plot.GetTimeSeries(
					ks.Keys[0].TTL,
					ids,
					query.Start,
					query.End,
					opers,
					true,
					true,
					false,
					ks.Keys[0].TSid,
				)
				if gerr != nil {
					rip.Fail(w, gerr)
					return
				}

				sumBytes += numBytes

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

	addProcessedBytesHeader(w, sumBytes)

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}
