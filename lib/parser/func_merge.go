package parser

import (
	"fmt"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

func parseMerge(exp string, tsdb *structs.TSDBquery) (string, gobol.Error) {

	params := parseParams(string(exp[5:]))

	if len(params) != 2 {
		return constants.StringsEmpty, errParams(
			"parseMerge",
			"merge needs 2 parameters: merge operation and a function",
			fmt.Errorf("merge expects 2 parameters but found %d: %v", len(params), params),
		)
	}

	tsdb.Aggregator = params[0]

	for _, oper := range tsdb.Order {
		if oper == "aggregation" {
			return constants.StringsEmpty, errDoubleFunc("parseMerge", "merge")
		}
	}

	tsdb.Order = append([]string{"aggregation"}, tsdb.Order...)

	return params[1], nil
}

func writeMerge(exp, operator string) string {
	return fmt.Sprintf("merge(%s,%s)", operator, exp)
}
