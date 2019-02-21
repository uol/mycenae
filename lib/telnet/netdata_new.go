package telnet

import (
	"encoding/json"
	"regexp"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/collector"
)

// RawNetdataHandler - handles netdata telnet format data
type RawNetdataHandler struct {
	tagsRegexp        *regexp.Regexp
	specialCharRegexp *regexp.Regexp
}

// NewRawNetdataHandler - creates the new handler
func NewRawNetdataHandler() *RawNetdataHandler {

	return &RawNetdataHandler{
		tagsRegexp:        regexp.MustCompile(tagRegexp),
		specialCharRegexp: regexp.MustCompile(tagValueReplacementRegexp),
	}
}

// Handle - extracts the points received by telnet
func (rnh *RawNetdataHandler) Handle(line string, pointCollector *collector.Collector, logger *zap.Logger, loggerFields []zapcore.Field) {

	if line == "" {
		return
	}

	pointJSON := netdataJSON{}

	err := json.Unmarshal([]byte(line), &pointJSON)
	if err != nil {
		logger.Error("error unmarshalling line: "+line, loggerFields...)
	}

	point := collector.TSDBpoint{
		Metric:    pointJSON.ChartType,
		Timestamp: pointJSON.Timestamp,
		Value:     &pointJSON.Value,
		Tags:      map[string]string{},
	}

	if pointJSON.ChartContext != "" {
		point.Tags["chart_context"] = pointJSON.ChartContext
	}

	if pointJSON.ChartFamily != "" {
		point.Tags["chart_family"] = rnh.specialCharRegexp.ReplaceAllString(pointJSON.ChartFamily, specialCharReplacement)
	}

	if pointJSON.ChartID != "" {
		point.Tags["chart_id"] = pointJSON.ChartID
	}

	if pointJSON.ChartName != "" {
		point.Tags["chart_name"] = pointJSON.ChartName
	}

	if pointJSON.Name != "" {
		point.Tags["name"] = rnh.specialCharRegexp.ReplaceAllString(pointJSON.Name, specialCharReplacement)
	}

	if pointJSON.ID != "" {
		point.Tags["id"] = pointJSON.ID
	}

	if pointJSON.HostName != "" {
		point.Tags["host"] = pointJSON.HostName
	}

	tagMatches := rnh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
	if len(tagMatches) > 0 {

		for i := 0; i < len(tagMatches); i++ {
			point.Tags[tagMatches[i][1]] = tagMatches[i][2]
		}
	}

	validatedPoint := &collector.Point{}

	err = pointCollector.MakePacket(validatedPoint, point, true)
	if err != nil {
		logger.Error("point validation failure in line: "+line, loggerFields...)
		return
	}

	pointCollector.HandlePacket(point, validatedPoint, true, "telnet-netdata-raw", nil)
}
