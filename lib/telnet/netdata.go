package telnet

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/buger/jsonparser"
	"github.com/json-iterator/go"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/structs"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

// Parse - parses the json bytes to the object fields
func (n *netdataJSON) Parse(data []byte) error {

	var err error

	if n.HostName, err = jsonparser.GetString(data, "hostname"); err != nil {
		return err
	}

	if n.DefaultTags, err = jsonparser.GetString(data, "host_tags"); err != nil {
		return err
	}

	if n.ChartID, err = jsonparser.GetString(data, "chart_id"); err != nil {
		return err
	}

	if n.ChartFamily, err = jsonparser.GetString(data, "chart_family"); err != nil {
		return err
	}

	if n.ChartContext, err = jsonparser.GetString(data, "chart_context"); err != nil {
		return err
	}

	if n.ChartType, err = jsonparser.GetString(data, "chart_type"); err != nil {
		return err
	}

	if n.ChartName, err = jsonparser.GetString(data, "chart_name"); err != nil {
		return err
	}

	if n.ID, err = jsonparser.GetString(data, "id"); err != nil {
		return err
	}

	if n.Units, err = jsonparser.GetString(data, "units"); err != nil {
		return err
	}

	if n.Name, err = jsonparser.GetString(data, "name"); err != nil {
		return err
	}

	if n.Value, err = jsonparser.GetFloat(data, "value"); err != nil {
		return err
	}

	if n.Timestamp, err = jsonparser.GetInt(data, "timestamp"); err != nil {
		return err
	}

	return nil
}

const tagRegexp string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/\*\+\']+)`
const tagValueReplacementRegexp string = `[^0-9A-Za-z-\._\%\&\#\;\/]+`
const specialCharReplacement string = "_"

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp        *regexp.Regexp
	specialCharRegexp *regexp.Regexp
	netdataTags       map[string]struct{}
	regexpCache       sync.Map
	mutex             *sync.Mutex
	cacheDuration     time.Duration
	collector         *collector.Collector
	logger            *zap.Logger
	loggerFields      []zapcore.Field
	sourceName        string
	telnetConfig      *structs.GlobalTelnetServerConfiguration
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(regexpCacheDuration string, collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration, logger *zap.Logger) *NetdataHandler {

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
		regexpCache:       sync.Map{},
		mutex:             &sync.Mutex{},
		logger:            logger,
		loggerFields: []zapcore.Field{
			zap.String("package", "telnet"),
			zap.String("func", "Handle"),
		},
		sourceName:   "telnet-netdata",
		telnetConfig: telnetConfig,
	}
}

// expireCachedRegexp - expires the cached regexp after some time
func (nh *NetdataHandler) expireCachedRegexp(regexp string) {

	go func() {

		<-time.After(nh.cacheDuration)

		nh.mutex.Lock()

		nh.regexpCache.Delete(regexp)

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
	err := pointJSON.Parse([]byte(line))
	if err != nil {
		if !nh.telnetConfig.SilenceLogs {
			nh.logger.Error("error unmarshalling line: "+line, nh.loggerFields...)
		}
		return
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
			if !nh.telnetConfig.SilenceLogs {
				nh.logger.Error("invalid netdata property to use set_metric: "+metricValue, nh.loggerFields...)
			}
			return
		}

		metricProperty = metricValue

		delete(tags, "%set_metric%")
	}

	if pluginMetricValue, switchPluginMetric := tags["%set_plugin_metric%"]; switchPluginMetric {

		list := strings.Split(strings.Trim(pluginMetricValue, "'"), "#")

		for i := 0; i < len(list); i++ {

			array := strings.Split(list[i], ";")

			if len(array) != 2 {
				if !nh.telnetConfig.SilenceLogs {
					nh.logger.Error("invalid set_plugin_metric value: "+pluginMetricValue, nh.loggerFields...)
				}
				return
			}

			if _, ok := nh.netdataTags[array[1]]; !ok {
				if !nh.telnetConfig.SilenceLogs {
					nh.logger.Error("invalid netdata property to use set_plugin_metric: "+pluginMetricValue, nh.loggerFields...)
				}
				return
			}

			var pluginMetricRegex *regexp.Regexp
			if compiledRegex, ok := nh.regexpCache.Load(array[0]); !ok {

				newCompiledRegex, err := regexp.Compile(array[0])
				if err != nil {
					if !nh.telnetConfig.SilenceLogs {
						nh.logger.Error("invalid set_plugin_metric regular expression: "+pluginMetricValue, nh.loggerFields...)
					}
					return
				}

				nh.regexpCache.Store(array[0], newCompiledRegex)

				nh.logger.Info(fmt.Sprintf("new regular expression was cached: %s", array[0]), nh.loggerFields...)

				nh.expireCachedRegexp(array[0])

				pluginMetricRegex = newCompiledRegex

			} else {

				pluginMetricRegex = compiledRegex.(*regexp.Regexp)
			}

			if pluginMetricRegex.MatchString(pointJSON.ChartID) {

				metricProperty = array[1]
			}
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

	validatedPoint, err := nh.collector.MakePacket(&point, true)
	if err != nil {
		if !nh.telnetConfig.SilenceLogs {
			nh.logger.Error(fmt.Sprintf("point validation failure in line: %s (error: %s)", line, err.Error()), nh.loggerFields...)
		}
		return
	}

	nh.collector.HandlePacket(validatedPoint, nh.sourceName)
}

// SourceName - returns the connection type name
func (nh *NetdataHandler) SourceName() string {
	return nh.sourceName
}
