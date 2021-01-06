package plot

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
)

// getSizeParameter - return parameter 'size'
func (plot *Plot) getSizeParameter(w http.ResponseWriter, q url.Values, function string) (int, bool) {

	sizeStr := q.Get("size")
	var err error
	size := plot.defaultMaxResults

	if sizeStr != constants.StringsEmpty {
		size, err = strconv.Atoi(sizeStr)
		if err != nil {
			rip.Fail(w, errParamSize(function, err))
			return size, true
		}
		if size <= 0 {
			rip.Fail(w, errParamSize(function, errors.New(constants.StringsEmpty)))
			return size, true
		}
	}

	return size, false
}

func (plot *Plot) ListTagsNumber(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "number")
}

func (plot *Plot) ListTagsText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTags(w, r, ps, "text")
}

func (plot *Plot) listTags(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.Fail(w, errNotFound("listTags"))
		return
	}

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

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		smap[constants.StringsKeyset] = "empty"
		rip.Fail(w, errNotFound("listMetrics"))
		return
	}

	smap[constants.StringsKeyset] = keyset

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
	plot.listMeta(w, r, ps, "meta")
}

func (plot *Plot) ListMetaText(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listMeta(w, r, ps, "metatext")
}

// getKeysetParameter - returns the keyset parameter
func (plot *Plot) getKeysetParameter(w http.ResponseWriter, r *http.Request, ps httprouter.Params, functionName string) (*string, bool) {

	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		err := errNotFound(functionName)
		rip.Fail(w, err)
		return nil, true
	}

	return &keyset, false
}

// getQueryParameter - extracts all query parameters found in the request
func (plot *Plot) getQueryParameter(w http.ResponseWriter, r *http.Request, ps httprouter.Params, functionName string) (*TSmeta, *string, bool) {

	keyset, fail := plot.getKeysetParameter(w, r, ps, functionName)
	if fail {
		return nil, nil, true
	}

	err := plot.validateKeyset(*keyset)
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

	if fromStr != constants.StringsEmpty {
		var err error
		from, err = strconv.Atoi(fromStr)
		if err != nil {
			rip.Fail(w, errParamFrom(function, err))
			return 0, true
		}
	}

	return from, false
}

func (plot *Plot) listMeta(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType string) {

	query, keyset, fail := plot.getQueryParameter(w, r, ps, "listMeta")
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

	if onlyidsStr != constants.StringsEmpty {
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

	query, keyset, fail := plot.getQueryParameter(w, r, ps, "deleteTS")
	if fail {
		return
	}

	q := r.URL.Query()

	size, fail := plot.getSizeParameter(w, q, "deleteTS")
	if fail {
		return
	}

	tags := map[string]string{}

	for _, tag := range query.Tags {
		tags[tag.Key] = tag.Value
	}

	keys, total, gerr := plot.ListMeta(*keyset, tsType, query.Metric, tags, false, size, 0)
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
			ttl := key.Tags["ttl"]
			gerr = plot.DeletePoint(key.TsId, ttl, *keyset, key.Metric)
			if gerr != nil {
				rip.Fail(w, gerr)
				return
			}
		}

		rip.SuccessJSON(w, http.StatusAccepted, out)
		return
	}
}

// ListNumberTagValuesByMetric - returns tag values filtered by metric
func (plot *Plot) ListNumberTagValuesByMetric(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTagsByMetric(w, r, ps, "meta", "ListNumberTagValuesByMetric", true)
}

// ListTextTagValuesByMetric - returns text tag values filtered by metric
func (plot *Plot) ListTextTagValuesByMetric(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTagsByMetric(w, r, ps, "metatext", "ListTextTagValuesByMetric", true)
}

// ListNumberTagKeysByMetric - returns tag keys filtered by metric
func (plot *Plot) ListNumberTagKeysByMetric(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTagsByMetric(w, r, ps, "meta", "ListNumberTagKeysByMetric", false)
}

// ListTextTagKeysByMetric - returns text tag keys filtered by metric
func (plot *Plot) ListTextTagKeysByMetric(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	plot.listTagsByMetric(w, r, ps, "metatext", "ListTextTagKeysByMetric", false)
}

// listTagsByMetric - returns tags filtered by metric
func (plot *Plot) listTagsByMetric(w http.ResponseWriter, r *http.Request, ps httprouter.Params, tsType, functionName string, filterValues bool) {

	keyset, fail := plot.getKeysetParameter(w, r, ps, functionName)
	if fail {
		return
	}

	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == constants.StringsEmpty {
		rip.Fail(w, errMandatoryParam(functionName, "metric"))
		return
	}

	size, fail := plot.getSizeParameter(w, q, functionName)
	if fail {
		return
	}

	var results []string
	var total int
	var gerr gobol.Error

	if filterValues {

		tag := q.Get("tag")
		if tag == constants.StringsEmpty {
			rip.Fail(w, errMandatoryParam(functionName, "tag"))
			return
		}

		value := q.Get("value")
		if value == constants.StringsEmpty {
			value = "*"
		}

		results, total, gerr = plot.persist.metaStorage.FilterTagValuesByMetricAndTag(*keyset, tsType, metric, tag, value, size)

	} else {

		tag := q.Get("tag")
		if tag == constants.StringsEmpty {
			tag = "*"
		}

		results, total, gerr = plot.persist.metaStorage.FilterTagKeysByMetric(*keyset, tsType, metric, tag, size)
	}

	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(results) == 0 {
		gerr := errNoContent(functionName)
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      results,
	}

	rip.SuccessJSON(w, http.StatusOK, out)

	return
}

// addProcessedBytesHeader - adds the number of processed bytes in the response header
func addProcessedBytesHeader(w http.ResponseWriter, numBytes uint32) {

	w.Header().Add("X-Processed-Bytes", strconv.FormatUint((uint64)(numBytes), 10))
}

const fmtDeleteQuery string = `DELETE FROM %v.ts_number_stamp WHERE id = ?`

func (plot *Plot) DeletePoint(tsID, ttl, keyset, metric string) gobol.Error {

	ttlInt, err := strconv.Atoi(ttl)
	if err != nil {
		return errPersist("DeletePoint", err)
	}
	keyspace := plot.keyspaceTTLMap[ttlInt]
	if err := plot.persist.cassandra.Query(
		fmt.Sprintf(fmtDeleteQuery, keyspace),
		tsID,
	).Exec(); err != nil {
		plot.statsDeleteMetaError("DeletePoint", keyset, metric)
		return errPersist("DeletePoint", err)
	}

	plot.statsDeleteMetaSuccess("DeletePoint", keyset, metric)
	return nil
}
