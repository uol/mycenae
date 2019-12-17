package telnet

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/uol/gobol/logh"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
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

const (
	propHostName              string = "hostname"
	propDefaultTags           string = "host_tags"
	propChartID               string = "chart_id"
	propChartFamily           string = "chart_family"
	propChartContext          string = "chart_context"
	propChartType             string = "chart_type"
	propChartName             string = "chart_name"
	propID                    string = "id"
	propName                  string = "name"
	propValue                 string = "value"
	propTimestamp             string = "timestamp"
	propHost                  string = "host"
	specialCharReplacement    string = "_"
	tagRegexp                 string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/\*\+\']+)`
	tagValueReplacementRegexp string = `[^0-9A-Za-z-\._\%\&\#\;\/]+`
	setMetricTag              string = "%set_metric%"
	setPluginMetric           string = "%set_plugin_metric%"
)

// Parse - parses the json bytes to the object fields
func (n *netdataJSON) Parse(data []byte) error {

	var err error

	if n.HostName, err = jsonparser.GetString(data, propHostName); err != nil {
		return err
	}

	if n.DefaultTags, err = jsonparser.GetString(data, propDefaultTags); err != nil {
		return err
	}

	if n.ChartID, err = jsonparser.GetString(data, propChartID); err != nil {
		return err
	}

	if n.ChartFamily, err = jsonparser.GetString(data, propChartFamily); err != nil {
		return err
	}

	if n.ChartContext, err = jsonparser.GetString(data, propChartContext); err != nil {
		return err
	}

	if n.ChartType, err = jsonparser.GetString(data, propChartType); err != nil {
		return err
	}

	if n.ChartName, err = jsonparser.GetString(data, propChartName); err != nil {
		return err
	}

	if n.ID, err = jsonparser.GetString(data, propID); err != nil {
		return err
	}

	if n.Name, err = jsonparser.GetString(data, propName); err != nil {
		return err
	}

	if n.Value, err = jsonparser.GetFloat(data, propValue); err != nil {
		return err
	}

	if n.Timestamp, err = jsonparser.GetInt(data, propTimestamp); err != nil {
		return err
	}

	return nil
}

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp        *regexp.Regexp
	specialCharRegexp *regexp.Regexp
	netdataTags       map[string]struct{}
	regexpCache       sync.Map
	mutex             *sync.Mutex
	cacheDuration     time.Duration
	collector         *collector.Collector
	logger            *logh.ContextualLogger
	sourceName        string
	telnetConfig      *structs.GlobalTelnetServerConfiguration
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(regexpCacheDuration string, collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration) *NetdataHandler {

	netdataTags := map[string]struct{}{
		propChartID:      struct{}{},
		propChartFamily:  struct{}{},
		propChartContext: struct{}{},
		propChartType:    struct{}{},
		propChartName:    struct{}{},
		propID:           struct{}{},
		propName:         struct{}{},
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
		logger:            logh.CreateContextualLogger(constants.StringsPKG, "telnet", constants.StringsFunc, "Handle"),
		sourceName:        "telnet-netdata",
		telnetConfig:      telnetConfig,
	}
}

// expireCachedRegexp - expires the cached regexp after some time
func (nh *NetdataHandler) expireCachedRegexp(regexp string) {

	go func() {
		<-time.After(nh.cacheDuration)

		nh.mutex.Lock()

		nh.regexpCache.Delete(regexp)

		if logh.InfoEnabled {
			nh.logger.Info().Msgf("regular expression expired from cache: %s", regexp)
		}

		nh.mutex.Unlock()
	}()
}

// Handle - extracts the points received by telnet
func (nh *NetdataHandler) Handle(line string) {

	if line == constants.StringsEmpty {
		return
	}

	pointJSON := netdataJSON{}
	err := pointJSON.Parse([]byte(line))
	if err != nil {
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Msgf("error unmarshalling line: %s", line)
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

	metricProperty := propChartID

	if metricValue, switchMetric := tags[setMetricTag]; switchMetric {

		if _, ok := nh.netdataTags[metricValue]; !ok {
			if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
				nh.logger.Error().Msgf("invalid netdata property to use set_metric: %s", metricValue)
			}
			return
		}

		metricProperty = metricValue

		delete(tags, setMetricTag)
	}

	if pluginMetricValue, switchPluginMetric := tags[setPluginMetric]; switchPluginMetric {

		list := strings.Split(strings.Trim(pluginMetricValue, "'"), "#")

		for i := 0; i < len(list); i++ {

			array := strings.Split(list[i], ";")

			if len(array) != 2 {
				if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					nh.logger.Error().Msgf("invalid set_plugin_metric value: %s", pluginMetricValue)
				}
				return
			}

			if _, ok := nh.netdataTags[array[1]]; !ok {
				if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					nh.logger.Error().Msgf("invalid netdata property to use set_plugin_metric: %s", pluginMetricValue)
				}
				return
			}

			var pluginMetricRegex *regexp.Regexp
			if compiledRegex, ok := nh.regexpCache.Load(array[0]); !ok {

				newCompiledRegex, err := regexp.Compile(array[0])
				if err != nil {
					if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
						nh.logger.Error().Msgf("invalid set_plugin_metric regular expression: %s", pluginMetricValue)
					}
					return
				}

				nh.regexpCache.Store(array[0], newCompiledRegex)

				if logh.InfoEnabled {
					nh.logger.Info().Msgf("new regular expression was cached: %s", array[0])
				}

				nh.expireCachedRegexp(array[0])

				pluginMetricRegex = newCompiledRegex

			} else {

				pluginMetricRegex = compiledRegex.(*regexp.Regexp)
			}

			if pluginMetricRegex.MatchString(pointJSON.ChartID) {

				metricProperty = array[1]
			}
		}

		delete(tags, setPluginMetric)
	}

	tags[propChartID] = pointJSON.ChartID

	if pointJSON.ChartContext != constants.StringsEmpty {
		tags[propChartContext] = pointJSON.ChartContext
	}

	if pointJSON.ChartFamily != constants.StringsEmpty {
		tags[propChartFamily] = nh.specialCharRegexp.ReplaceAllString(pointJSON.ChartFamily, specialCharReplacement)
	}

	if pointJSON.ChartType != constants.StringsEmpty {
		tags[propChartType] = pointJSON.ChartType
	}

	if pointJSON.ChartName != constants.StringsEmpty {
		tags[propChartName] = pointJSON.ChartName
	}

	if pointJSON.Name != constants.StringsEmpty {
		tags[propName] = nh.specialCharRegexp.ReplaceAllString(pointJSON.Name, specialCharReplacement)
	}

	if pointJSON.ID != constants.StringsEmpty {
		tags[propID] = pointJSON.ID
	}

	if pointJSON.HostName != constants.StringsEmpty {
		tags[propHost] = pointJSON.HostName
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
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Err(err).Msgf("point validation failure in line: %s", line)
		}
		return
	}

	nh.collector.HandlePacket(validatedPoint, nh.sourceName)
}

// SourceName - returns the connection type name
func (nh *NetdataHandler) SourceName() string {
	return nh.sourceName
}
