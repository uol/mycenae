package telnet

import (
	"encoding/json"
	"regexp"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/structs"
)

// netdataJSON - a JSON line from netdata packet
type netdataJSON struct {
	HostName     string  `json:"hostname"`
	DefaultTags  string  `json:"host_tags"`
	ChartID      string  `json:"chart_id"`
	ChartFamily  string  `json:"chart_family"`
	ChartContext string  `json:"chart_context"`
	ChartType    string  `json:"chart_type"`
	Units        string  `json:"units"`
	Name         string  `json:"name"`
	Value        float64 `json:"value"`
	Timestamp    int64   `json:"timestamp"`
}

// getJSONValue - returns one JSON property value by its name
func (nh *NetdataHandler) getJSONValue(property *string, data *netdataJSON) string {

	switch *property {
	case "chart_family":
		return data.ChartFamily
	case "chart_context":
		return data.ChartContext
	case "chart_type":
		return data.ChartType
	case "units":
		return data.Units
	case "name":
		return data.Name
	default:
		return data.ChartID
	}
}

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp     *regexp.Regexp
	replacements   []structs.NetdataMetricReplacement
	replaceMetrics bool
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(netdataMetricReplacements []structs.NetdataMetricReplacement) *NetdataHandler {

	nh := &NetdataHandler{
		tagsRegexp: regexp.MustCompile(`([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`),
	}

	numReplacements := len(netdataMetricReplacements)
	if numReplacements > 0 {

		nh.replacements = netdataMetricReplacements
		nh.replaceMetrics = true
	}

	return nh
}

// Handle - extracts the points received by telnet
func (nh *NetdataHandler) Handle(line string, pointCollector *collector.Collector, logger *zap.Logger, loggerFields []zapcore.Field) {

	if line == "" {
		return
	}

	pointJSON := netdataJSON{}

	err := json.Unmarshal([]byte(line), &pointJSON)
	if err != nil {
		logger.Error("error unmarshalling line: "+line, loggerFields...)
	}

	point := collector.TSDBpoint{
		Metric:    pointJSON.ChartID,
		Timestamp: pointJSON.Timestamp,
		Value:     &pointJSON.Value,
		Tags:      map[string]string{},
	}

	if nh.replaceMetrics {

		for _, replacement := range nh.replacements {

			if nh.getJSONValue(&replacement.LookForPropertyName, &pointJSON) == replacement.LookForPropertyValue {

				point.Metric = nh.getJSONValue(&replacement.PropertyAsNewMetric, &pointJSON)
				if len(replacement.NewTagName) > 0 {
					point.Tags[replacement.NewTagName] = nh.getJSONValue(&replacement.NewTagValue, &pointJSON)
				}
			}
		}
	}

	tagMatches := nh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
	if len(tagMatches) > 0 {

		for i := 0; i < len(tagMatches); i++ {
			point.Tags[tagMatches[i][1]] = tagMatches[i][2]
		}
	}

	if pointJSON.Name != "" {
		point.Tags["name"] = strings.Replace(pointJSON.Name, " ", "_", -1)
	}

	if pointJSON.HostName != "" {
		point.Tags["host"] = pointJSON.HostName
	}

	validatedPoint := &collector.Point{}

	err = pointCollector.MakePacket(validatedPoint, point, true)
	if err != nil {
		logger.Error("point validation failure in line: "+line, loggerFields...)
		return
	}

	pointCollector.HandlePacket(point, validatedPoint, true, "telnet-netdata", nil)
}
