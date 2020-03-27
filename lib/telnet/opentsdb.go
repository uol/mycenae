package telnet

import (
	"regexp"
	"strconv"

	"github.com/uol/gobol"
	"github.com/uol/logh"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"
)

//
// Implements the opentsdb telnet handler.
// author: rnojiri
//

// TelnetFormatTagsRegexp - contains the regexp to parse the tags
const TelnetFormatTagsRegexp string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`

// OpenTSDBHandler - handles opentsdb telnet format data
type OpenTSDBHandler struct {
	formatRegexp      *regexp.Regexp
	tagsRegexp        *regexp.Regexp
	collector         *collector.Collector
	logger            *logh.ContextualLogger
	sourceName        string
	telnetConfig      *structs.GlobalTelnetServerConfiguration
	validationService *validation.Service
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler(collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration, validationService *validation.Service) *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp:      regexp.MustCompile(`put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9Ee\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)`),
		tagsRegexp:        regexp.MustCompile(TelnetFormatTagsRegexp),
		collector:         collector,
		sourceName:        "telnet-opentsdb",
		logger:            logh.CreateContextualLogger(constants.StringsPKG, "telnet", constants.StringsFunc, "Handle"),
		telnetConfig:      telnetConfig,
		validationService: validationService,
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string) {

	if line == constants.StringsEmpty {
		if !otsdbh.telnetConfig.SilenceLogs && logh.DebugEnabled {
			otsdbh.logger.Debug().Msg("empty line received")
		}
		return
	}

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("invalid pattern: %s", line)
		}
		return
	}

	tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
	if len(tagMatches) == 0 {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable tags found: %s", line)
		}
		return
	}

	var err error
	var gerr gobol.Error
	point := structs.TSDBpoint{}

	gerr = otsdbh.validationService.ValidateProperty(matches[1], validation.MetricType)
	if gerr != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("invalid metric: %s", line)
		}
		return
	}

	point.Metric = matches[1]

	point.Timestamp, err = strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable timestamp found: %s", line)
		}
		return
	}

	point.Timestamp, gerr = otsdbh.validationService.ValidateTimestamp(point.Timestamp)
	if gerr != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("invalid timestamp: %s", point.Timestamp)
		}
		return
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable float number found: %s", line)
		}
		return
	}

	point.Value = &value

	point.Tags = []structs.TSDBTag{}
	ttlFound := false
	ksidFound := false

	for i := 0; i < len(tagMatches); i++ {

		switch tagMatches[i][1] {
		case constants.StringsTTL:
			ttl, ttlStr, gerr := otsdbh.validationService.ParseTTL(tagMatches[i][2])
			if gerr != nil {
				if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					otsdbh.logger.Error().Err(gerr).Msgf("invalid ttl: %s", line)
				}
				return
			}
			point.TTL = ttl
			tagMatches[i][2] = ttlStr
			ttlFound = true
		case constants.StringsKSID:
			gerr = otsdbh.validationService.ValidateKeyset(tagMatches[i][2])
			if gerr != nil {
				if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					otsdbh.logger.Error().Err(gerr).Msgf("invalid ksid: %s", line)
				}
				return
			}
			point.Keyset = tagMatches[i][2]
			ksidFound = true
		default:
			gerr = otsdbh.validationService.ValidateProperty(tagMatches[i][1], validation.TagKeyType)
			if gerr != nil {
				if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					otsdbh.logger.Error().Err(gerr).Msgf("invalid key: %s", line)
				}
				return
			}

			gerr = otsdbh.validationService.ValidateProperty(tagMatches[i][2], validation.TagValueType)
			if gerr != nil {
				if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
					otsdbh.logger.Error().Err(gerr).Msgf("invalid value: %s", line)
				}
				return
			}
		}

		tag := structs.TSDBTag{
			Name:  tagMatches[i][1],
			Value: tagMatches[i][2],
		}

		point.Tags = append(point.Tags, tag)
	}

	gerr = otsdbh.validationService.ValidateTags(&point)
	if gerr != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Err(gerr).Msgf("tags validation failure: %s", line)
		}
		return
	}

	if !ksidFound {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Err(gerr).Msgf("no ksid tag found: %s", line)
		}
		return
	}

	if !ttlFound {
		ttlTag, ttl := otsdbh.validationService.GetDefaultTTLTag()
		point.Tags = append(point.Tags, *ttlTag)
		point.TTL = ttl
	}

	validatedPoint, err := otsdbh.collector.MakePacket(&point, true)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("point validation failure: %s", line)
		}
		return
	}

	otsdbh.collector.HandlePacket(validatedPoint, otsdbh.sourceName)
}

// SourceName - returns the connection type name
func (otsdbh *OpenTSDBHandler) SourceName() string {
	return otsdbh.sourceName
}
