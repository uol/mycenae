package parser

import (
	"fmt"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

func parseFilter(exp string, tsdb *structs.TSDBquery) (string, gobol.Error) {

	params := parseParams(string(exp[6:]))

	if len(params) != 2 {
		return constants.StringsEmpty, errParams(
			"parseFilter",
			"filter needs 2 parameters: >, >=, <, <=, ==, != followed by a number and a function",
			fmt.Errorf("filter expects 2 parameters but found %d: %v", len(params), params),
		)
	}

	tsdb.FilterValue = params[0]

	for _, oper := range tsdb.Order {
		if oper == "filterValue" {
			return constants.StringsEmpty, errDoubleFunc("parseFilter", "filter")
		}
	}

	tsdb.Order = append([]string{"filterValue"}, tsdb.Order...)

	return params[1], nil
}

func writeFilter(exp, filterValue string) string {
	if filterValue != constants.StringsEmpty {
		return fmt.Sprintf("filter(%s,%s)", filterValue, exp)
	}
	return exp
}
