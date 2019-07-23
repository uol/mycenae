package telnet

import (
	"regexp"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/structs"
)

// TelnetFormatTagsRegexp - contains the regexp to parse the tags
const TelnetFormatTagsRegexp string = `([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`

// OpenTSDBHandler - handles opentsdb telnet format data
type OpenTSDBHandler struct {
	formatRegexp *regexp.Regexp
	tagsRegexp   *regexp.Regexp
	collector    *collector.Collector
	logger       *zap.Logger
	loggerFields []zapcore.Field
	sourceName   string
	telnetConfig *structs.GlobalTelnetServerConfiguration
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler(collector *collector.Collector, telnetConfig *structs.GlobalTelnetServerConfiguration, logger *zap.Logger) *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp: regexp.MustCompile(`put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9E\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)`),
		tagsRegexp:   regexp.MustCompile(TelnetFormatTagsRegexp),
		collector:    collector,
		loggerFields: []zapcore.Field{
			zap.String("package", "telnet"),
			zap.String("func", "Handle"),
		},
		sourceName:   "telnet-opentsdb",
		logger:       logger,
		telnetConfig: telnetConfig,
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string) {

	if line == "" {
		return
	}

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		if !otsdbh.telnetConfig.SilenceLogs {
			otsdbh.logger.Error("this line does not follows the accepted pattern: "+line, otsdbh.loggerFields...)
		}
		return
	}

	tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
	if len(tagMatches) == 0 {
		otsdbh.logger.Error("no parseable tags found in line: "+line, otsdbh.loggerFields...)
		return
	}

	var err error
	point := collector.TSDBpoint{}
	point.Tags = map[string]string{}

	for i := 0; i < len(tagMatches); i++ {
		point.Tags[tagMatches[i][1]] = tagMatches[i][2]
	}

	point.Metric = matches[1]

	point.Timestamp, err = strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs {
			otsdbh.logger.Error("no parseable timestamp found in line: "+line, otsdbh.loggerFields...)
		}
		return
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs {
			otsdbh.logger.Error("no parseable float number found in line: "+line, otsdbh.loggerFields...)
		}
		return
	}

	point.Value = &value

	validatedPoint, err := otsdbh.collector.MakePacket(&point, true)
	if err != nil {
		if !otsdbh.telnetConfig.SilenceLogs {
			otsdbh.logger.Error("point validation failure in line: "+line, otsdbh.loggerFields...)
		}
		return
	}

	otsdbh.collector.HandlePacket(validatedPoint, otsdbh.sourceName)
}

// SourceName - returns the connection type name
func (otsdbh *OpenTSDBHandler) SourceName() string {
	return otsdbh.sourceName
}
