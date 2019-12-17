package parser

import (
	"fmt"
	"strings"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

//ParseExpression parses a timeseries query expression and returns a TSDB query struct with the expression values
func ParseExpression(exp string, tsdb *structs.TSDBquery) (relative string, err gobol.Error) {
	relative, err = parseExpression(exp, tsdb)
	if err != nil {
		return relative, err
	}
	cleanOrder := []string{}
	for _, oper := range tsdb.Order {
		if oper != "query" && oper != "groupBy" {
			cleanOrder = append(cleanOrder, oper)
		}
	}
	tsdb.Order = cleanOrder
	return relative, nil
}

func parseExpression(exp string, tsdb *structs.TSDBquery) (relative string, err gobol.Error) {

	var name []rune

	exp = strings.Replace(exp, constants.StringsWhitespace, constants.StringsEmpty, -1)

	for _, s := range exp {
		if string(s) == "(" {
			break
		}
		name = append(name, s)
	}

	switch string(name) {
	case "query":
		relative, err = parseQuery(exp, tsdb)
		exp = constants.StringsEmpty
	case "merge":
		exp, err = parseMerge(exp, tsdb)
	case "downsample":
		exp, err = parseDownsample(exp, tsdb)
	case "groupBy":
		exp, err = parseGroup(exp, tsdb)
	case "rate":
		exp, err = parseRate(exp, tsdb)
	case "filter":
		exp, err = parseFilter(exp, tsdb)
	default:
		return constants.StringsEmpty, errUnkFunc(fmt.Sprintf("unkown function %s", string(name)))
	}
	if err != nil {
		return constants.StringsEmpty, err
	}
	if exp != constants.StringsEmpty {
		relative, err = parseExpression(exp, tsdb)
	}

	return relative, err
}

//CompileExpression writes an expression given a TSDB query struct
func CompileExpression(tsQueries []structs.TSDBqueryPayload) (exps []string) {

	for _, tsQuery := range tsQueries {
		for _, query := range tsQuery.Queries {

			exp := writeQuery(query.Metric, tsQuery.Relative, query.Filters)

			for _, operation := range query.Order {

				switch operation {
				case "aggregation":
					exp = writeMerge(exp, query.Aggregator)
				case "downsample":
					exp = writeDownsample(exp, query.Downsample)
				case "rate":
					exp = writeRate(exp, query.Rate, query.RateOptions)
				case "filterValue":
					exp = writeFilter(exp, query.FilterValue)
				}

			}

			exp = writeGroup(exp, query.Filters)

			exps = append(exps, exp)

		}
	}

	return exps
}
