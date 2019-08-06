package plot

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/utils"

	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/metadata"

	"github.com/julienschmidt/httprouter"
)

type queryParameters struct {
	keyspace    string
	since       int64
	until       int64
	tsids       []string
	metadataMap map[string]RawDataMetadata
}

// RawDataQuery - returns the raw query
func (plot *Plot) RawDataQuery(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	rip.AddStatsMap(r, map[string]string{"path": "/api/query/raw"})

	defer r.Body.Close()

	rawQuery := &RawDataQuery{}
	gerr := rawQuery.Parse(r)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	qp := queryParameters{}
	var err error
	var ttl int
	var ok bool

	if _, ok := rawQuery.Tags[rawDataQueryTTL]; ok {
		ttl, err = strconv.Atoi(rawQuery.Tags[rawDataQueryTTL])
		if err != nil {
			ttl = plot.defaultTTL
		}
	} else {
		ttl = plot.defaultTTL
	}

	if qp.keyspace, ok = plot.keyspaceTTLMap[ttl]; !ok {
		rip.Fail(w, errValidationS("RawDataQuery", fmt.Sprintf("ttl %d do not exists", ttl)))
		return
	}

	qp.since, gerr = plot.getNowMinusDuration(rawQuery.Since)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if rawQuery.Until != "" {
		qp.until, gerr = plot.getNowMinusDuration(rawQuery.Until)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
	} else {
		qp.until = utils.GetTimeNoMillis()
	}

	metadataQuery := metadata.Query{
		Metric: rawQuery.Metric,
	}

	if rawQuery.Type == rawDataQueryTextType {
		metadataQuery.MetaType = "metatext"
	} else {
		metadataQuery.MetaType = "meta"
	}

	metadataQuery.Tags = make([]metadata.QueryTag, len(rawQuery.Tags))
	i := 0
	for k, v := range rawQuery.Tags {

		if k == rawDataQueryKSID {
			continue
		}

		metadataQuery.Tags[i] = metadata.QueryTag{
			Key:    k,
			Values: []string{v},
		}
		i++
	}

	metadataArray, _, gerr := plot.persist.metaStorage.FilterMetadata(rawQuery.Tags[rawDataQueryKSID], &metadataQuery, 0, plot.MaxTimeseries)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	qp.tsids = make([]string, len(metadataArray))
	qp.metadataMap = map[string]RawDataMetadata{}

	for i, m := range metadataArray {
		qp.tsids[i] = m.ID
		qp.metadataMap[m.ID] = RawDataMetadata{
			Metric: m.Metric,
			Tags:   map[string]string{},
		}

		for i, k := range m.TagKey {
			qp.metadataMap[m.ID].Tags[k] = m.TagValue[i]
		}
	}

	var results interface{}
	var numBytes uint32
	if rawQuery.Type == rawDataQueryTextType {
		results, numBytes, gerr = plot.getRawTextPoints(&qp)
	} else {
		results, numBytes, gerr = plot.getRawNumberPoints(&qp)
	}

	addProcessedBytesHeader(w, numBytes)

	if numBytes == 0 {

		rip.Success(w, http.StatusNoContent, nil)
		return
	}

	rip.SuccessJSON(w, http.StatusOK, results)

	return
}

// getNowMinusDuration - returns the time now minus the duration
func (plot *Plot) getNowMinusDuration(strDuration string) (int64, gobol.Error) {

	duration, err := time.ParseDuration(strDuration)
	if err != nil {
		return 0, errValidationE("getNowMinusDuration", err)
	}

	duration = -duration

	pastTime, err := utils.MilliToSeconds(time.Now().Add(duration).Unix())
	if err != nil {
		return 0, errValidationE("getNowMinusDuration", err)
	}

	return pastTime, nil
}

// getRawTextPoints - returns all texts points filtered by the query
func (plot *Plot) getRawTextPoints(qp *queryParameters) (interface{}, uint32, gobol.Error) {

	textTSMap, bytes, err := plot.persist.GetTST(qp.keyspace, qp.tsids, qp.since, qp.until, nil, plot.maxBytesLimit)
	if err != nil {
		return nil, 0, errInternalServer("getRawTextPoints", err)
	}

	if bytes == 0 {
		return nil, 0, nil
	}

	mainResult := RawDataQueryTextResults{}
	mainResult.Results = make([]RawDataQueryTextPoints, 0)

	i := 0
	total := 0
	for tsid, points := range textTSMap {

		numPoints := len(points)
		if numPoints == 0 {
			continue
		}

		total++

		rawTextPoint := RawDataQueryTextPoints{
			Metadata: qp.metadataMap[tsid],
			Texts:    make([]RawDataTextPoint, numPoints),
		}

		for j, p := range points {
			rawTextPoint.Texts[j] = RawDataTextPoint{
				Timestamp: p.Date,
				Text:      p.Value,
			}
		}

		mainResult.Results = append(mainResult.Results, rawTextPoint)

		i++
	}

	mainResult.Total = total

	return mainResult, bytes, nil
}

// getRawNumberPoints - returns all number points filtered by the query
func (plot *Plot) getRawNumberPoints(qp *queryParameters) (interface{}, uint32, gobol.Error) {

	textTSMap, bytes, err := plot.persist.GetTS(qp.keyspace, qp.tsids, qp.since, qp.until, false, plot.maxBytesLimit)
	if err != nil {
		return nil, 0, errInternalServer("getRawNumberPoints", err)
	}

	if bytes == 0 {
		return nil, 0, nil
	}

	mainResult := RawDataQueryNumberResults{}
	mainResult.Results = make([]RawDataQueryNumberPoints, 0)

	i := 0
	total := 0
	for tsid, points := range textTSMap {

		numPoints := len(points)
		if numPoints == 0 {
			continue
		}

		total++

		rawNumberPoint := RawDataQueryNumberPoints{
			Metadata: qp.metadataMap[tsid],
			Values:   make([]RawDataNumberPoint, numPoints),
		}

		for j, p := range points {
			rawNumberPoint.Values[j] = RawDataNumberPoint{
				Timestamp: p.Date,
				Value:     p.Value,
			}
		}

		mainResult.Results = append(mainResult.Results, rawNumberPoint)

		i++
	}

	mainResult.Total = total

	return mainResult, bytes, nil
}
