package parser

import (
	"fmt"
	"strings"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

func parseDownsample(exp string, tsdb *structs.TSDBquery) (string, gobol.Error) {

	params := parseParams(string(exp[10:]))

	if len(params) != 4 {
		return constants.StringsEmpty, errParams(
			"parseDownsample",
			"downsample needs 4 parameters: downsample operation, downsample period, fill option and a function",
			fmt.Errorf("downsample expects 4 parameters but found %d: %v", len(params), params),
		)
	}

	tsdb.Downsample = fmt.Sprintf("%s-%s-%s", params[0], params[1], params[2])

	for _, oper := range tsdb.Order {
		if oper == "downsample" {
			return constants.StringsEmpty, errDoubleFunc("parseDownsample", "downsample")
		}
	}

	tsdb.Order = append([]string{"downsample"}, tsdb.Order...)

	return params[3], nil
}

func writeDownsample(exp, dsInfo string) string {
	if dsInfo != constants.StringsEmpty {
		info := strings.Split(dsInfo, "-")
		if len(info) == 2 {
			info = append(info, "none")
		}
		exp = fmt.Sprintf("downsample(%s,%s,%s,%s)", info[0], info[1], info[2], exp)
	}
	return exp
}
