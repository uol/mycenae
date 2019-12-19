package telnet

import (
	"regexp"
	"strconv"

	"github.com/uol/gobol/logh"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

// TelnetFormatTagsRegexp - contains the regexp to parse the tags
const TelnetFormatTagsRegexp string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`

// OpenTSDBHandler - handles opentsdb telnet format data
type OpenTSDBHandler struct {
	formatRegexp *regexp.Regexp
	tagsRegexp   *regexp.Regexp
	collector    *collector.Collector
	logger       *logh.ContextualLogger
	sourceName   string
	telnetConfig *structs.GlobalTelnetServerConfiguration
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler(collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration) *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp: regexp.MustCompile(`put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9E\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)`),
		tagsRegexp:   regexp.MustCompile(TelnetFormatTagsRegexp),
		collector:    collector,
		sourceName:   "telnet-opentsdb",
		logger:       logh.CreateContextualLogger(constants.StringsPKG, "telnet", constants.StringsFunc, "Handle"),
		telnetConfig: telnetConfig,
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string) {

	if line == constants.StringsEmpty {
		return
	}

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("this line does not follows the accepted pattern: %s", line)
		}
		return
	}

	tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
	if len(tagMatches) == 0 {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable tags found in line: %s", line)
		}
		return
	}

	var err error
	point := structs.TSDBpoint{}
	point.Tags = make([]structs.TSDBTag, len(tagMatches))

	for i := 0; i < len(tagMatches); i++ {
		point.Tags[i] = structs.TSDBTag{
			Name:  tagMatches[i][1],
			Value: tagMatches[i][2],
		}
	}

	point.Metric = matches[1]

	point.Timestamp, err = strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable timestamp found in line: %s", line)
		}
		return
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("no parseable float number found in line: %s", line)
		}
		return
	}

	point.Value = &value

	validatedPoint, err := otsdbh.collector.MakePacket(&point, true)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs && logh.ErrorEnabled {
			otsdbh.logger.Error().Msgf("point validation failure in line: %s", line)
		}
		return
	}

	otsdbh.collector.HandlePacket(validatedPoint, otsdbh.sourceName)
}

// SourceName - returns the connection type name
func (otsdbh *OpenTSDBHandler) SourceName() string {
	return otsdbh.sourceName
}
