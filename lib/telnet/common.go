package telnet

import "regexp"

var keysetGroupRegexp = regexp.MustCompile(`ksid=([0-9A-Za-z-\._\%\&\#\;\/]+)`)

func extractKeysetValue(line string) string {

	groups := keysetGroupRegexp.FindStringSubmatch(line)
	if len(groups) == 2 {
		return groups[1]
	}

	return ""
}
