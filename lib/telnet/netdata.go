package telnet

import (
	"encoding/json"
	"regexp"
	"strings"

	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/collector"
)

// netdataJSON - a JSON line from netdata packet
type netdataJSON struct {
	HostName    string  `json:"hostname"`
	DefaultTags string  `json:"host_tags"`
	ChartID     string  `json:"chart_id"`
	Name        string  `json:"name"`
	Value       float64 `json:"value"`
	Timestamp   int64   `json:"timestamp"`
}

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp *regexp.Regexp
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler() *NetdataHandler {

	return &NetdataHandler{
		tagsRegexp: regexp.MustCompile(`([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`),
	}
}

// Handle - extracts the points received by telnet
func (nh *NetdataHandler) Handle(data *string, pointCollector *collector.Collector, logger *zap.Logger) {

	lines := strings.Split(*data, "\n")
	numLines := len(lines)

	for i := 0; i < numLines; i++ {

		if lines[i] == "" {
			continue
		}

		pointJSON := netdataJSON{}

		err := json.Unmarshal([]byte(lines[i]), &pointJSON)
		if err != nil {
			logger.Debug("error unmarshalling line: " + lines[i])
		}

		point := collector.TSDBpoint{
			Metric:    pointJSON.ChartID,
			Timestamp: pointJSON.Timestamp,
			Value:     &pointJSON.Value,
			Tags:      map[string]string{},
		}

		tagMatches := nh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
		if len(tagMatches) > 0 {

			for i := 0; i < len(tagMatches); i++ {
				point.Tags[tagMatches[i][1]] = tagMatches[i][2]
			}
		}

		if pointJSON.Name != "" {
			point.Tags["name"] = pointJSON.Name
		}

		if pointJSON.HostName != "" {
			point.Tags["host"] = pointJSON.HostName
		}

		validatedPoint := &collector.Point{}

		err = pointCollector.MakePacket(validatedPoint, point, true)
		if err != nil {
			logger.Debug("point validation failure in line: " + lines[i])
			continue
		}

		pointCollector.HandlePacket(point, validatedPoint, true, "netdata", nil)
	}
}
