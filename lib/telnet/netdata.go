package telnet

import (
	"regexp"
	"sync"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/logh"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"
)

//
// Implements the netdata json telnet handler.
// author: rnojiri
//

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
	Keyset       string  `json:"-"` //only for quick identification
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

	cMsgFKeysetNotFound       string = "no keyset found for host: %s"
	cMsgErrParsingJSONProp    string = "error parsing json property"
	cMsgFErrInvalidSpecialCmd string = "invalid netdata property to use set_metric: %s"
)

// Parse - parses the json bytes to the object fields (returns the first identifier found)
func (n *netdataJSON) Parse(data []byte, silenceLogs bool, logger *logh.ContextualLogger) gobol.Error {

	var err error

	if n.HostName, err = jsonparser.GetString(data, propHostName); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.DefaultTags, err = jsonparser.GetString(data, propDefaultTags); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	n.Keyset = extractKeysetValue(n.DefaultTags)
	if len(n.Keyset) == 0 {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ChartID, err = jsonparser.GetString(data, propChartID); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ChartFamily, err = jsonparser.GetString(data, propChartFamily); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ChartContext, err = jsonparser.GetString(data, propChartContext); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ChartType, err = jsonparser.GetString(data, propChartType); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ChartName, err = jsonparser.GetString(data, propChartName); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.ID, err = jsonparser.GetString(data, propID); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.Name, err = jsonparser.GetString(data, propName); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.Value, err = jsonparser.GetFloat(data, propValue); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	if n.Timestamp, err = jsonparser.GetInt(data, propTimestamp); err != nil {
		return n.getParsingError(err, silenceLogs, logger)
	}

	return nil
}

func (n *netdataJSON) getParsingError(err error, silenceLogs bool, logger *logh.ContextualLogger) gobol.Error {
	if !silenceLogs && logh.ErrorEnabled {
		logger.Error().Err(err).Msg(cMsgErrParsingJSONProp)
	}
	return errDataFormatParse
}

// NetdataHandler - handles netdata telnet format data
type NetdataHandler struct {
	tagsRegexp        *regexp.Regexp
	specialCharRegexp *regexp.Regexp
	netdataTags       map[string]bool
	regexpCache       sync.Map
	mutex             *sync.Mutex
	cacheDuration     time.Duration
	collector         *collector.Collector
	logger            *logh.ContextualLogger
	configuration     *structs.TelnetServerConfiguration
	validationService *validation.Service
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(regexpCacheDuration string, collector *collector.Collector, configuration *structs.TelnetServerConfiguration, validationService *validation.Service) *NetdataHandler {

	netdataTags := map[string]bool{
		propChartID:      true,
		propChartFamily:  true,
		propChartContext: true,
		propChartType:    true,
		propChartName:    true,
		propID:           true,
		propName:         true,
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
		configuration:     configuration,
		validationService: validationService,
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
func (nh *NetdataHandler) Handle(line, ip string) bool {

	if len(line) == 0 {
		if !nh.configuration.SilenceLogs && logh.DebugEnabled {
			nh.logger.Debug().Msg(cMsgEmptyLine)
		}
		return false
	}

	var gerr gobol.Error
	pointJSON := netdataJSON{}

	gerr = pointJSON.Parse([]byte(line), nh.configuration.SilenceLogs, nh.logger)
	if gerr != nil {
		logAndStats(nh, gerr, cFuncParse, pointJSON.Keyset, ip, cMsgFInvalidLineContent, line)
		return false
	}

	point := structs.TSDBpoint{
		Value: &pointJSON.Value,
		Tags:  []structs.TSDBTag{},
	}

	point.Timestamp, gerr = nh.validationService.ValidateTimestamp(pointJSON.Timestamp)
	if gerr != nil {
		logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, pointJSON.HostName, cMsgFInvalidTimestamp, point.Timestamp)
		return false
	}

	ttlFound := false
	ksidFound := false
	tagMatches := nh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
	metricPropertyReplacement := propChartID

	if len(tagMatches) > 0 {

		for i := 0; i < len(tagMatches); i++ {

			if setMetricTag == tagMatches[i][1] {

				if _, ok := nh.netdataTags[tagMatches[i][2]]; !ok {
					logAndStats(nh, errSpecialCommandFormat, cFuncHandle, pointJSON.Keyset, ip, cMsgFErrInvalidSpecialCmd, tagMatches[i][2])
					continue
				}

				metricPropertyReplacement = tagMatches[i][2]

			} else {

				switch tagMatches[i][1] {
				case constants.StringsTTL:
					ttl, ttlStr, gerr := nh.validationService.ParseTTL(tagMatches[i][2])
					if gerr != nil {
						logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, ip, cMsgFInvalidTTL, line)
						continue
					}
					point.TTL = ttl
					tagMatches[i][2] = ttlStr
					ttlFound = true
				case constants.StringsKSID:
					gerr = nh.validationService.ValidateKeyset(tagMatches[i][2])
					if gerr != nil {
						logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, ip, cMsgFInvalidKSID, line)
						continue
					}
					point.Keyset = tagMatches[i][2]
					ksidFound = true
				default:
					gerr = nh.validationService.ValidateProperty(tagMatches[i][1], validation.TagKeyType)
					if gerr != nil {
						logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, ip, cMsgFInvalidKey, line)
						continue
					}

					gerr = nh.validationService.ValidateProperty(tagMatches[i][2], validation.TagValueType)
					if gerr != nil {
						logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, ip, cMsgFInvalidValue, line)
						continue
					}
				}

				tag := structs.TSDBTag{
					Name:  tagMatches[i][1],
					Value: tagMatches[i][2],
				}

				point.Tags = append(point.Tags, tag)
			}
		}
	}

	if !ksidFound {
		logAndStats(nh, validation.ErrNoKeysetTag, cFuncHandle, pointJSON.Keyset, pointJSON.HostName, cMsgFKSIDTagNotFound, line)
		return false
	}

	if !ttlFound {
		ttlTag, ttl := nh.validationService.GetDefaultTTLTag()
		point.Tags = append(point.Tags, *ttlTag)
		point.TTL = ttl
	}

	point.Tags = append(point.Tags, structs.TSDBTag{Name: propChartID, Value: pointJSON.ChartID})

	if pointJSON.ChartContext != constants.StringsEmpty {
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propChartContext, Value: pointJSON.ChartContext})
	}

	if pointJSON.ChartFamily != constants.StringsEmpty {
		pointJSON.ChartFamily = nh.specialCharRegexp.ReplaceAllString(pointJSON.ChartFamily, specialCharReplacement)
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propChartFamily, Value: pointJSON.ChartFamily})
	}

	if pointJSON.ChartType != constants.StringsEmpty {
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propChartType, Value: pointJSON.ChartType})
	}

	if pointJSON.ChartName != constants.StringsEmpty {
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propChartName, Value: pointJSON.ChartName})
	}

	if pointJSON.Name != constants.StringsEmpty {
		pointJSON.Name = nh.specialCharRegexp.ReplaceAllString(pointJSON.Name, specialCharReplacement)
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propName, Value: pointJSON.Name})
	}

	if pointJSON.ID != constants.StringsEmpty {
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propID, Value: pointJSON.ID})
	}

	if pointJSON.HostName != constants.StringsEmpty {
		point.Tags = append(point.Tags, structs.TSDBTag{Name: propHost, Value: pointJSON.HostName})
	}

	gerr = nh.validationService.ValidateTags(&point)
	if gerr != nil {
		logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, pointJSON.HostName, cMsgFInvalidTags, line)
		return false
	}

	metric := constants.StringsEmpty
	switch metricPropertyReplacement {
	case propChartID:
		metric = pointJSON.ChartID
	case propChartContext:
		metric = pointJSON.ChartContext
	case propChartFamily:
		metric = pointJSON.ChartFamily
	case propChartType:
		metric = pointJSON.ChartType
	case propChartName:
		metric = pointJSON.Name
	case propID:
		metric = pointJSON.ID
	case propHost:
		metric = pointJSON.HostName
	default:
		metric = pointJSON.ChartID
	}

	point.Metric = metric

	gerr = nh.validationService.ValidateProperty(point.Metric, validation.MetricType)
	if gerr != nil {
		logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, pointJSON.HostName, cMsgFInvalidMetric, line)
		return false
	}

	packet, gerr := nh.collector.MakePacket(&point, true)
	if gerr != nil {
		logAndStats(nh, gerr, cFuncHandle, pointJSON.Keyset, pointJSON.HostName, cMsgFPointCreationError, line)
		return false
	}

	nh.collector.HandlePacket(packet, nh.GetSourceType())

	return true
}

// GetSourceType - returns the source type
func (nh *NetdataHandler) GetSourceType() *constants.SourceType {
	return constants.SourceTypeTelnetNetdata
}

// GetLogger - returns the logger
func (nh *NetdataHandler) GetLogger() *logh.ContextualLogger {
	return nh.logger
}

// GetValidationService - returns the validation service instance
func (nh *NetdataHandler) GetValidationService() *validation.Service {
	return nh.validationService
}

// GetConfiguration - returns this handler configuration
func (nh *NetdataHandler) GetConfiguration() *structs.TelnetServerConfiguration {
	return nh.configuration
}
