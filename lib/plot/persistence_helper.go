package plot

import (
	"strings"

	"github.com/uol/mycenae/lib/constants"
)

// buildInGroup - build the group query part
func (persist *persistence) buildInGroup(keys []string) string {

	value := constants.StringsEmpty
	for _, v := range keys {
		value += "'"
		value += v
		value += "',"
	}

	return strings.TrimRight(value, ",")
}
