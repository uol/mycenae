package plot

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/parser"
	"github.com/uol/mycenae/lib/structs"
)

func (plot *Plot) ExpressionCheckPOST(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	expQuery := ExpQuery{}

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	plot.ExpressionCheck(w, expQuery)
}

func (plot *Plot) ExpressionCheckGET(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	expQuery := ExpQuery{
		Expression: r.URL.Query().Get("exp"),
	}

	plot.ExpressionCheck(w, expQuery)
}

func (plot *Plot) ExpressionCheck(w http.ResponseWriter, expQuery ExpQuery) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("ExpressionCheck")
		rip.Fail(w, gerr)
		return
	}

	tsdb := structs.TSDBquery{}

	relative, gerr := parser.ParseExpression(expQuery.Expression, &tsdb)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	payload := structs.TSDBqueryPayload{
		Queries: []structs.TSDBquery{
			tsdb,
		},
		Relative: relative,
	}

	gerr = payload.Validate()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	rip.Success(w, http.StatusOK, nil)
	return

}

func (plot *Plot) ExpressionQueryPOST(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/query/expression", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ExpressionQueryPOST"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/query/expression", "keyspace": keyspace})

	expQuery := ExpQuery{}

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	plot.expressionQuery(w, r, keyspace, expQuery)
}

func (plot *Plot) ExpressionQueryGET(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/query/expression", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ExpressionQueryGET"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/query/expression", "keyspace": keyspace})

	expQuery := ExpQuery{
		Expression: r.URL.Query().Get("exp"),
	}

	plot.expressionQuery(w, r, keyspace, expQuery)
}

func (plot *Plot) expressionQuery(w http.ResponseWriter, r *http.Request, keyspace string, expQuery ExpQuery) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("expressionQuery")
		rip.Fail(w, gerr)
		return
	}

	strTUUID, found, gerr := plot.memcached.GetKeyspace(keyspace)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if !found {
		gerr := errNotFound("expressionQuery")
		rip.Fail(w, gerr)
		return
	}

	tuuid, err := strconv.ParseBool(strTUUID)
	if err != nil {
		gerr := errValidationE("expressionQuery", err)
		rip.Fail(w, gerr)
		return
	}

	tsdb := structs.TSDBquery{}

	relative, gerr := parser.ParseExpression(expQuery.Expression, &tsdb)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	tsuid := false
	tsuidStr := r.URL.Query().Get("tsuid")
	if tsuidStr != "" {
		b, err := strconv.ParseBool(tsuidStr)
		if err != nil {
			gerr := errValidationE("expressionQuery", err)
			rip.Fail(w, gerr)
			return
		}
		tsuid = b
	}

	payload := structs.TSDBqueryPayload{
		Queries: []structs.TSDBquery{
			tsdb,
		},
		Relative:   relative,
		ShowTSUIDs: tsuid,
	}

	gerr = payload.Validate()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	resps, gerr := plot.getTimeseries(keyspace, tuuid, payload)
	if err != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(resps) == 0 {
		rip.SuccessJSON(w, http.StatusOK, []string{})
		return
	}

	rip.SuccessJSON(w, http.StatusOK, resps)
	return
}

func (plot *Plot) ExpressionParsePOST(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	expQuery := ExpParse{}

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if expQuery.Keyspace != "" {
		rip.AddStatsMap(r, map[string]string{"keyspace": expQuery.Keyspace})
	}

	plot.expressionParse(w, expQuery)
}

func (plot *Plot) ExpressionParseGET(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	query := r.URL.Query()

	expQuery := ExpParse{
		Expression: query.Get("exp"),
		Keyspace:   query.Get("keyspace"),
	}

	if expQuery.Keyspace != "" {
		rip.AddStatsMap(r, map[string]string{"keyspace": expQuery.Keyspace})
	}

	expandStr := query.Get("expand")
	if expandStr == "" {
		expandStr = "false"
	}
	expand, err := strconv.ParseBool(expandStr)
	if err != nil {
		gerr := errValidationE("ExpressionParseGET", err)
		rip.Fail(w, gerr)
		return
	}

	expQuery.Expand = expand

	plot.expressionParse(w, expQuery)
}

func (plot *Plot) expressionParse(w http.ResponseWriter, expQuery ExpParse) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("expressionParse")
		rip.Fail(w, gerr)
		return
	}

	if expQuery.Expand {

		if expQuery.Keyspace == "" {
			gerr := errValidationS("expressionParse", `When expand true, Keyspace can not be empty`)
			rip.Fail(w, gerr)
			return
		}

		_, found, gerr := plot.memcached.GetKeyspace(expQuery.Keyspace)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if !found {
			gerr := errValidationS("expressionParse", `keyspace not found`)
			rip.Fail(w, gerr)
			return
		}

	}

	tsdb := structs.TSDBquery{}

	relative, gerr := parser.ParseExpression(expQuery.Expression, &tsdb)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	payload := structs.TSDBqueryPayload{
		Queries: []structs.TSDBquery{
			tsdb,
		},
		Relative: relative,
	}

	gerr = payload.Validate()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if !expQuery.Expand {
		rip.SuccessJSON(w, http.StatusOK, []structs.TSDBqueryPayload{payload})
		return
	}

	payloadExp, gerr := plot.expandStruct(expQuery.Keyspace, payload)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(payloadExp) == 0 {
		rip.Success(w, http.StatusNoContent, nil)
		return
	}

	rip.SuccessJSON(w, http.StatusOK, payloadExp)
	return
}

func (plot *Plot) ExpressionCompile(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	tsdb := structs.TSDBqueryPayload{}

	gerr := rip.FromJSON(r, &tsdb)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if tsdb.Relative == "" {
		gerr := errValidationS("ExpressionCompile", "field relative can not be empty")
		rip.Fail(w, gerr)
		return
	}

	if tsdb.Start != 0 || tsdb.End != 0 {
		gerr := errValidationS("ExpressionCompile", "expression compile supports only relative times, start and end fields should be empty")
		rip.Fail(w, gerr)
		return
	}

	exps := parser.CompileExpression([]structs.TSDBqueryPayload{tsdb})
	rip.SuccessJSON(w, http.StatusOK, exps)
	return
}

func (plot *Plot) ExpressionExpandPOST(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	expQuery := ExpQuery{}

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/expression/expand", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ExpressionExpandPOST"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/expression/expand", "keyspace": keyspace})

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	plot.expressionExpand(w, keyspace, expQuery)
}

func (plot *Plot) ExpressionExpandGET(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	expQuery := ExpQuery{
		Expression: r.URL.Query().Get("exp"),
	}

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/expression/expand", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ExpressionExpandGET"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/expression/expand", "keyspace": keyspace})

	plot.expressionExpand(w, keyspace, expQuery)
}

func (plot *Plot) expressionExpand(w http.ResponseWriter, keyspace string, expQuery ExpQuery) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("expressionExpand")
		rip.Fail(w, gerr)
		return
	}

	_, found, gerr := plot.memcached.GetKeyspace(keyspace)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if !found {
		gerr := errNotFound("expressionExpand")
		rip.Fail(w, gerr)
		return
	}

	tsdb := structs.TSDBquery{}

	relative, gerr := parser.ParseExpression(expQuery.Expression, &tsdb)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	payload := structs.TSDBqueryPayload{
		Queries: []structs.TSDBquery{
			tsdb,
		},
		Relative: relative,
	}

	gerr = payload.Validate()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	payloadExp, gerr := plot.expandStruct(keyspace, payload)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(payloadExp) == 0 {
		rip.Success(w, http.StatusNoContent, nil)
		return
	}

	exps := parser.CompileExpression(payloadExp)

	sort.Strings(exps)

	rip.SuccessJSON(w, http.StatusOK, exps)
	return
}

func (plot *Plot) expandStruct(
	keyspace string,
	tsdbq structs.TSDBqueryPayload,
) (groupQueries []structs.TSDBqueryPayload, err gobol.Error) {

	tsdb := tsdbq.Queries[0]

	needExpand := false

	tagMap := map[string][]string{}

	for _, filter := range tsdb.Filters {
		if filter.GroupBy {
			needExpand = true
		}
		if _, ok := tagMap[filter.Tagk]; ok {
			tagMap[filter.Tagk] = append(tagMap[filter.Tagk], filter.Filter)
		} else {
			tagMap[filter.Tagk] = []string{filter.Filter}
		}
	}

	if needExpand {

		tsobs, total, gerr := plot.MetaFilterOpenTSDB(
			keyspace,
			"",
			tsdb.Metric,
			tsdb.Filters,
			int64(10000),
		)
		if gerr != nil {
			return groupQueries, gerr
		}
		if total > 10000 {
			return groupQueries, errValidationS(
				"expandStruct",
				fmt.Sprintf(
					"expand exedded the maximum allowed number of timeseries. max is 10000 and the query returned %d",
					total,
				),
			)
		}

		groups := plot.GetGroups(tsdb.Filters, tsobs)

		for _, tsobjs := range groups {

			filtersPlain := []structs.TSDBfilter{}

			for _, filter := range tsdb.Filters {
				if !filter.GroupBy {
					filtersPlain = append(filtersPlain, filter)
				}
			}

			for _, filter := range tsdb.Filters {

				if filter.GroupBy {

					found := false

					for _, f := range filtersPlain {
						if filter.Tagk == f.Tagk && tsobjs[0].Tags[filter.Tagk] == f.Filter {
							found = true
						}
					}

					if !found {
						filtersPlain = append(filtersPlain, structs.TSDBfilter{
							Ftype:   "wildcard",
							Tagk:    filter.Tagk,
							Filter:  tsobjs[0].Tags[filter.Tagk],
							GroupBy: false,
						})
					}
				}
			}

			query := structs.TSDBqueryPayload{
				Relative: tsdbq.Relative,
				Queries: []structs.TSDBquery{
					{
						Aggregator:  tsdb.Aggregator,
						Downsample:  tsdb.Downsample,
						Metric:      tsdb.Metric,
						Tags:        map[string]string{},
						Rate:        tsdb.Rate,
						RateOptions: tsdb.RateOptions,
						Order:       tsdb.Order,
						FilterValue: tsdb.FilterValue,
						Filters:     filtersPlain,
					},
				},
			}

			groupQueries = append(groupQueries, query)
		}

	} else {
		groupQueries = append(groupQueries, tsdbq)
	}

	return groupQueries, err
}
