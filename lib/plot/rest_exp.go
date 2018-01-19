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

	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/query/expression", "keyset": "empty"})
		rip.Fail(w, errNotFound("ExpressionQueryPOST"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/query/expression", "keyset": keyset})

	expQuery := ExpQuery{}

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	plot.expressionQuery(w, r, keyset, expQuery)
}

func (plot *Plot) ExpressionQueryGET(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/query/expression", "keyset": "empty"})
		rip.Fail(w, errNotFound("ExpressionQueryGET"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/query/expression", "keyset": keyset})

	expQuery := ExpQuery{
		Expression: r.URL.Query().Get("exp"),
	}

	plot.expressionQuery(w, r, keyset, expQuery)
}

func (plot *Plot) expressionQuery(w http.ResponseWriter, r *http.Request, keyset string, expQuery ExpQuery) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("expressionQuery")
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

	resps, gerr := plot.getTimeseries(keyset, payload)
	if gerr != nil {
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

func (plot *Plot) ExpressionParsePOST(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	expQuery := ExpParse{}

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if expQuery.Keyset != "" {
		rip.AddStatsMap(r, map[string]string{"ksid": expQuery.Keyset})
	}

	plot.expressionParse(w, expQuery)
}

func (plot *Plot) ExpressionParseGET(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	query := r.URL.Query()

	expQuery := ExpParse{
		Expression: query.Get("exp"),
		Keyset:   query.Get("ksid"),
	}

	if expQuery.Keyset != "" {
		rip.AddStatsMap(r, map[string]string{"ksid": expQuery.Keyset})
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

		if expQuery.Keyset == "" {
			gerr := errValidationS("expressionParse", `When expand true, ksid can not be empty`)
			rip.Fail(w, gerr)
			return
		}

		found, gerr := plot.keySet.KeySetExists(expQuery.Keyset)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		if !found {
			gerr := errValidationS("expressionParse", `ksid not found`)
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

	payloadExp, gerr := plot.expandStruct(expQuery.Keyset, payload)
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

	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/expression/expand", "keyset": "empty"})
		rip.Fail(w, errNotFound("ExpressionExpandPOST"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/expression/expand", "keyset": keyset})

	gerr := rip.FromJSON(r, &expQuery)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	plot.expressionExpand(w, keyset, expQuery)
}

func (plot *Plot) ExpressionExpandGET(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	expQuery := ExpQuery{
		Expression: r.URL.Query().Get("exp"),
	}

	keyset := ps.ByName("keyset")
	if keyset == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/expression/expand", "keyset": "empty"})
		rip.Fail(w, errNotFound("ExpressionExpandGET"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/expression/expand", "keyset": keyset})

	plot.expressionExpand(w, keyset, expQuery)
}

func (plot *Plot) expressionExpand(w http.ResponseWriter, keyset string, expQuery ExpQuery) {

	if expQuery.Expression == "" {
		gerr := errEmptyExpression("expressionExpand")
		rip.Fail(w, gerr)
		return
	}

	found, gerr := plot.keySet.KeySetExists(keyset)
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

	payloadExp, gerr := plot.expandStruct(keyset, payload)
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
	keyset string,
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
			keyset,
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
