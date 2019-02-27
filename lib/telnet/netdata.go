package telnet

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/collector"
)

// netdataJSON - a JSON line from netdata packet
type netdataJSON struct {
	HostName     string  `json:"hostname"`
	DefaultTags  string  `json:"host_tags"`
	ChartID      string  `json:"chart_id"`
	ChartFamily  string  `json:"chart_family"`
	ChartContext string  `json:"chart_context"`
	ChartType    string  `json:"chart_type"`
	ChartName    string  `json:"chart_name"`
	ID           string  `json:"id"`
	Units        string  `json:"units"`
	Name         string  `json:"name"`
	Value        float64 `json:"value"`
	Timestamp    int64   `json:"timestamp"`
}

const tagRegexp string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/\*\+\']+)`
const tagValueReplacementRegexp string = `[^0-9A-Za-z-\._\%\&\#\;\/]+`
const specialCharReplacement string = "_"

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp        *regexp.Regexp
	specialCharRegexp *regexp.Regexp
	netdataTags       map[string]struct{}
	regexpCache       map[string]*regexp.Regexp
	mutex             *sync.Mutex
	cacheDuration     time.Duration
	collector         *collector.Collector
	logger            *zap.Logger
	loggerFields      []zapcore.Field
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(regexpCacheDuration string, collector *collector.Collector, logger *zap.Logger) *NetdataHandler {

	netdataTags := map[string]struct{}{
		"chart_id":      struct{}{},
		"chart_family":  struct{}{},
		"chart_context": struct{}{},
		"chart_type":    struct{}{},
		"chart_name":    struct{}{},
		"id":            struct{}{},
		"name":          struct{}{},
	}

	cacheDuration, err := time.ParseDuration(regexpCacheDuration)
	if err != nil {
		panic(err)
	}

	return &NetdataHandler{
		tagsRegexp:        regexp.MustCompile(tagRegexp),
		specialCharRegexp: regexp.MustCompile(tagValueReplacementRegexp),
		netdataTags:       netdataTags,
		cacheDuration:     cacheDuration,
		collector:         collector,
		regexpCache:       map[string]*regexp.Regexp{},
		mutex:             &sync.Mutex{},
		logger:            logger,
		loggerFields: []zapcore.Field{
			zap.String("package", "telnet"),
			zap.String("func", "Handle"),
		},
	}
}

// expireCachedRegexp - expires the cached regexp after some time
func (nh *NetdataHandler) expireCachedRegexp(regexp string) {

	go func() {

		<-time.After(nh.cacheDuration)

		nh.mutex.Lock()

		delete(nh.regexpCache, regexp)

		nh.logger.Info(fmt.Sprintf("regular expression expired from cache: %s", regexp), nh.loggerFields...)

		nh.mutex.Unlock()
	}()
}

// Handle - extracts the points received by telnet
func (nh *NetdataHandler) Handle(line string) {

	if line == "" {
		return
	}

	pointJSON := netdataJSON{}

	err := json.Unmarshal([]byte(line), &pointJSON)
	if err != nil {
		nh.logger.Error("error unmarshalling line: "+line, nh.loggerFields...)
	}

	tags := map[string]string{}

	tagMatches := nh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
	if len(tagMatches) > 0 {
		for i := 0; i < len(tagMatches); i++ {
			tags[tagMatches[i][1]] = tagMatches[i][2]
		}
	}

	metricProperty := "chart_id"

	if metricValue, switchMetric := tags["%set_metric%"]; switchMetric {

		if _, ok := nh.netdataTags[metricValue]; !ok {
			nh.logger.Error("invalid netdata property to use set_metric: "+metricValue, nh.loggerFields...)
			return
		}

		metricProperty = metricValue

		delete(tags, "%set_metric%")
	}

	if pluginMetricValue, switchPluginMetric := tags["%set_plugin_metric%"]; switchPluginMetric {

		array := strings.Split(strings.Trim(pluginMetricValue, "'"), ";")

		if len(array) != 2 {
			nh.logger.Error("invalid set_plugin_metric value: "+pluginMetricValue, nh.loggerFields...)
			return
		}

		if _, ok := nh.netdataTags[array[1]]; !ok {
			nh.logger.Error("invalid netdata property to use set_plugin_metric: "+pluginMetricValue, nh.loggerFields...)
			return
		}

		var pluginMetricRegex *regexp.Regexp
		if compiledRegex, ok := nh.regexpCache[array[0]]; !ok {

			var err error
			compiledRegex, err = regexp.Compile(array[0])
			if err != nil {
				nh.logger.Error("invalid set_plugin_metric regular expression: "+pluginMetricValue, nh.loggerFields...)
				return
			}

			nh.regexpCache[array[0]] = compiledRegex

			nh.logger.Info(fmt.Sprintf("new regular expression was cached: %s", array[0]), nh.loggerFields...)

			nh.expireCachedRegexp(array[0])

			pluginMetricRegex = compiledRegex
		} else {
			pluginMetricRegex = compiledRegex
		}

		if pluginMetricRegex.MatchString(pointJSON.ChartID) {
			metricProperty = array[1]
		}

		delete(tags, "%set_plugin_metric%")
	}

	tags["chart_id"] = pointJSON.ChartID

	if pointJSON.ChartContext != "" {
		tags["chart_context"] = pointJSON.ChartContext
	}

	if pointJSON.ChartFamily != "" {
		tags["chart_family"] = nh.specialCharRegexp.ReplaceAllString(pointJSON.ChartFamily, specialCharReplacement)
	}

	if pointJSON.ChartType != "" {
		tags["chart_type"] = pointJSON.ChartType
	}

	if pointJSON.ChartName != "" {
		tags["chart_name"] = pointJSON.ChartName
	}

	if pointJSON.Name != "" {
		tags["name"] = nh.specialCharRegexp.ReplaceAllString(pointJSON.Name, specialCharReplacement)
	}

	if pointJSON.ID != "" {
		tags["id"] = pointJSON.ID
	}

	if pointJSON.HostName != "" {
		tags["host"] = pointJSON.HostName
	}

	newMetric := tags[metricProperty]

	delete(tags, metricProperty)

	point := collector.TSDBpoint{
		Metric:    newMetric,
		Timestamp: pointJSON.Timestamp,
		Value:     &pointJSON.Value,
		Tags:      tags,
	}

	validatedPoint := &collector.Point{}

	err = nh.collector.MakePacket(validatedPoint, point, true)
	if err != nil {
		nh.logger.Error("point validation failure in line: "+line, nh.loggerFields...)
		return
	}

	nh.collector.HandlePacket(point, validatedPoint, true, "telnet-netdata", nil)
}
