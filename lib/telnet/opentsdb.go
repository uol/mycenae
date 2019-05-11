package telnet

import (
	"regexp"
	"strconv"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/collector"
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
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler(collector *collector.Collector, logger *zap.Logger) *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp: regexp.MustCompile(`put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9E\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)`),
		tagsRegexp:   regexp.MustCompile(TelnetFormatTagsRegexp),
		collector:    collector,
		loggerFields: []zapcore.Field{
			zap.String("package", "telnet"),
			zap.String("func", "Handle"),
		},
		sourceName: "telnet-opentsdb",
		logger:     logger,
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string) {

	if line == "" {
		return
	}

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		otsdbh.logger.Error("this line does not follows the accepted pattern: "+line, otsdbh.loggerFields...)
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
		otsdbh.logger.Error("no parseable timestamp found in line: "+line, otsdbh.loggerFields...)
		return
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		otsdbh.logger.Error("no parseable float number found in line: "+line, otsdbh.loggerFields...)
		return
	}

	point.Value = &value

	validatedPoint := &collector.Point{}

	err = otsdbh.collector.MakePacket(validatedPoint, point, true)
	if err != nil {
		otsdbh.logger.Error("point validation failure in line: "+line, otsdbh.loggerFields...)
		return
	}

	otsdbh.collector.HandlePacket(point, validatedPoint, true, otsdbh.sourceName, nil)
}

// SourceName - returns the connection type name
func (otsdbh *OpenTSDBHandler) SourceName() string {
	return otsdbh.sourceName
}
