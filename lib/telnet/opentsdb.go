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

const (
	cMsgFNoParseableTagsFound  string = "error reading line content: %s"
	cMsgFNoParseableValueFound string = "error parsing value: %s"
)

// OpenTSDBHandler - handles opentsdb telnet format data
type OpenTSDBHandler struct {
	formatRegexp      *regexp.Regexp
	tagsRegexp        *regexp.Regexp
	collector         *collector.Collector
	logger            *logh.ContextualLogger
	configuration     *structs.TelnetServerConfiguration
	validationService *validation.Service
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler(collector *collector.Collector, configuration *structs.TelnetServerConfiguration, validationService *validation.Service) *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp:      regexp.MustCompile(`put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9Ee\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)`),
		tagsRegexp:        regexp.MustCompile(`([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`),
		collector:         collector,
		logger:            logh.CreateContextualLogger(constants.StringsPKG, "telnet", constants.StringsFunc, "Handle"),
		configuration:     configuration,
		validationService: validationService,
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string, ip string) bool {

	if len(line) == 0 {
		if !otsdbh.configuration.SilenceLogs && logh.DebugEnabled {
			otsdbh.logger.Debug().Msg(cMsgEmptyLine)
		}
		return true
	}

	keyset := extractKeysetValue(line)

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		logAndStats(otsdbh, errDataFormatParse, cFuncHandle, keyset, ip, cMsgFInvalidLineContent, line)
		return false
	}

	tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
	if len(tagMatches) == 0 {
		logAndStats(otsdbh, errDataFormatParse, cFuncHandle, keyset, ip, cMsgFNoParseableTagsFound, line)
		return false
	}

	var err error
	var gerr gobol.Error
	point := structs.TSDBpoint{}
	point.Tags = []structs.TSDBTag{}
	ttlFound := false
	ksidFound := false

	for i := 0; i < len(tagMatches); i++ {

		switch tagMatches[i][1] {
		case constants.StringsTTL:
			ttl, ttlStr, gerr := otsdbh.validationService.ParseTTL(tagMatches[i][2])
			if gerr != nil {
				logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidTTL, line)
				return false
			}
			point.TTL = ttl
			tagMatches[i][2] = ttlStr
			ttlFound = true
		case constants.StringsKSID:
			gerr = otsdbh.validationService.ValidateKeyset(tagMatches[i][2])
			if gerr != nil {
				logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidKSID, line)
				return false
			}
			point.Keyset = tagMatches[i][2]
			ksidFound = true
		default:
			gerr = otsdbh.validationService.ValidateProperty(tagMatches[i][1], validation.TagKeyType)
			if gerr != nil {
				logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidKey, line)
				return false
			}

			gerr = otsdbh.validationService.ValidateProperty(tagMatches[i][2], validation.TagValueType)
			if gerr != nil {
				logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidValue, line)
				return false
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
		logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidTags, line)
		return false
	}

	if !ksidFound {
		logAndStats(otsdbh, validation.ErrNoKeysetTag, cFuncHandle, keyset, ip, cMsgFKSIDTagNotFound, line)
		return false
	}

	if !ttlFound {
		ttlTag, ttl := otsdbh.validationService.GetDefaultTTLTag()
		point.Tags = append(point.Tags, *ttlTag)
		point.TTL = ttl
	}

	gerr = otsdbh.validationService.ValidateProperty(matches[1], validation.MetricType)
	if gerr != nil {
		logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidMetric, line)
		return false
	}

	point.Metric = matches[1]

	point.Timestamp, err = strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidTimestamp, line)
		return false
	}

	point.Timestamp, gerr = otsdbh.validationService.ValidateTimestamp(point.Timestamp)
	if gerr != nil {
		logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFInvalidTimestamp, line)
		return false
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		logAndStats(otsdbh, validation.ErrParsingValue, cFuncHandle, keyset, ip, cMsgFInvalidValue, line)
		return false
	}

	point.Value = &value

	validatedPoint, gerr := otsdbh.collector.MakePacket(&point, true)
	if err != nil {
		logAndStats(otsdbh, gerr, cFuncHandle, keyset, ip, cMsgFPointCreationError, line)
		return false
	}

	otsdbh.collector.HandlePacket(validatedPoint, otsdbh.GetSourceType())

	return true
}

// GetSourceType - returns the source type
func (otsdbh *OpenTSDBHandler) GetSourceType() *constants.SourceType {
	return constants.SourceTypeTelnetOpenTSDB
}

// GetLogger - returns the logger
func (otsdbh *OpenTSDBHandler) GetLogger() *logh.ContextualLogger {
	return otsdbh.logger
}

// GetValidationService - returns the validation service instance
func (otsdbh *OpenTSDBHandler) GetValidationService() *validation.Service {
	return otsdbh.validationService
}

// SilenceLogs - checks the configuration to silence all validation logs
func (otsdbh *OpenTSDBHandler) SilenceLogs() bool {
	return otsdbh.configuration.SilenceLogs
}

// GetConfiguration - returns this handler configuration
func (otsdbh *OpenTSDBHandler) GetConfiguration() *structs.TelnetServerConfiguration {
	return otsdbh.configuration
}
