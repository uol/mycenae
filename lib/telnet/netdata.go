package telnet

import (
	"regexp"
	"sync"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/logh"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"
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
	validationService *validation.Service
}

// NewNetdataHandler - creates the new handler
func NewNetdataHandler(regexpCacheDuration string, collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration, validationService *validation.Service) *NetdataHandler {

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

	var gerr gobol.Error

	point := structs.TSDBpoint{
		Value: &pointJSON.Value,
		Tags:  []structs.TSDBTag{},
	}

	point.Timestamp, gerr = nh.validationService.ValidateTimestamp(pointJSON.Timestamp)
	if gerr != nil {
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Msgf("invalid timestamp: %s", point.Timestamp)
		}
		return
	}

	ttlFound := false
	ksidFound := false
	tagMatches := nh.tagsRegexp.FindAllStringSubmatch(pointJSON.DefaultTags, -1)
	metricPropertyReplacement := propChartID

	if len(tagMatches) > 0 {
		for i := 0; i < len(tagMatches); i++ {

			if setMetricTag == tagMatches[i][1] {

				if _, ok := nh.netdataTags[tagMatches[i][2]]; !ok {
					if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
						nh.logger.Error().Msgf("invalid netdata property to use set_metric: %s", tagMatches[i][2])
					}
					return
				}

				metricPropertyReplacement = tagMatches[i][2]

			} else {

				switch tagMatches[i][1] {
				case constants.StringsTTL:
					ttl, ttlStr, gerr := nh.validationService.ParseTTL(tagMatches[i][2])
					if gerr != nil {
						if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
							nh.logger.Error().Err(gerr).Msgf("invalid ttl: %s", line)
						}
						return
					}
					point.TTL = ttl
					tagMatches[i][2] = ttlStr
					ttlFound = true
				case constants.StringsKSID:
					gerr = nh.validationService.ValidateKeyset(tagMatches[i][2])
					if gerr != nil {
						if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
							nh.logger.Error().Err(gerr).Msgf("invalid ksid: %s", line)
						}
						return
					}
					point.Keyset = tagMatches[i][2]
					ksidFound = true
				default:
					gerr = nh.validationService.ValidateProperty(tagMatches[i][1], validation.TagKeyType)
					if gerr != nil {
						if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
							nh.logger.Error().Err(gerr).Msgf("invalid key: %s", line)
						}
						return
					}

					gerr = nh.validationService.ValidateProperty(tagMatches[i][2], validation.TagValueType)
					if gerr != nil {
						if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
							nh.logger.Error().Err(gerr).Msgf("invalid value: %s", line)
						}
						return
					}
				}

				tag := structs.TSDBTag{
					Name:  tagMatches[i][1],
					Value: tagMatches[i][2],
				}

				dup := false
				for i, k := range point.Tags {
					if k.Name == tag.Name {
						point.Tags[i].Value = tag.Value
						dup = true
						break
					}
				}

				if !dup {
					point.Tags = append(point.Tags, tag)
				}
			}
		}
	}

	if !ksidFound {
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Err(gerr).Msgf("no ksid tag found: %s", line)
		}
		return
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
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Err(gerr).Msgf("tags validation failure: %s", line)
		}
		return
	}

	metric := ""
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
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Err(gerr).Msgf("invalid metric: %s", line)
		}
		return
	}

	packet, err := nh.collector.MakePacket(&point, true)
	if err != nil {
		if !nh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			nh.logger.Error().Err(err).Msgf("point validation failure: %s", line)
		}
		return
	}

	nh.collector.HandlePacket(packet, nh.sourceName)
}

// SourceName - returns the connection type name
func (nh *NetdataHandler) SourceName() string {
	return nh.sourceName
}
