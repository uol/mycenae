package plot

import (
	"strings"
)

// buildInGroup - build the group query part
func (persist *persistence) buildInGroup(keys []string) string {

	value := ""
	for _, v := range keys {
		value += "'"
		value += v
		value += "',"
	}

	return strings.TrimRight(value, ",")
}
