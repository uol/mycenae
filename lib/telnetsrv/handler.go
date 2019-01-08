package telnetsrv

import (
	"strconv"
	"strings"

	"github.com/uol/mycenae/lib/collector"
)

// handlePoints - extracts the points received by telnet
func (server *Server) handlePoints(data *string) {

	lines := strings.Split(*data, "\n")
	numLines := len(lines)

	for i := 0; i < numLines; i++ {

		if lines[i] == "" {
			continue
		}

		matches := server.formatRegexp.FindStringSubmatch(lines[i])
		if len(matches) != 5 {
			server.logger.Warn("this line does not follows the accepted pattern: " + lines[i])
			continue
		}

		tagMatches := server.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
		if len(tagMatches) == 0 {
			server.logger.Warn("no parseable tags found in line: " + lines[i])
			continue
		}

		var err error
		point := collector.TSDBpoint{}
		point.Tags = map[string]string{}

		for i := 0; i < len(tagMatches); i++ {
			point.Tags[tagMatches[i][1]] = tagMatches[i][2]
		}

		point.Metric = matches[1]

		point.Timestamp, err = strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			server.logger.Warn("no parseable timestamp found in line: " + lines[i])
			continue
		}

		value, err := strconv.ParseFloat(matches[3], 64)
		if err != nil {
			server.logger.Warn("no parseable float number found in line: " + lines[i])
			continue
		}

		point.Value = &value

		validatedPoint := &collector.Point{}

		err = server.collector.MakePacket(validatedPoint, point, true)
		if err != nil {
			server.logger.Warn("point validation failure in line: " + lines[i])
			continue
		}

		server.collector.HandlePacket(point, validatedPoint, true, "telnet", nil)
	}
}
