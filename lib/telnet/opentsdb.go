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
}

// NewOpenTSDBHandler - creates the new handler
func NewOpenTSDBHandler() *OpenTSDBHandler {

	return &OpenTSDBHandler{
		formatRegexp: regexp.MustCompile(`^put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9E\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)$`),
		tagsRegexp:   regexp.MustCompile(TelnetFormatTagsRegexp),
	}
}

// Handle - extracts the points received by telnet
func (otsdbh *OpenTSDBHandler) Handle(line string, pointCollector *collector.Collector, logger *zap.Logger, loggerFields []zapcore.Field) {

	if line == "" {
		return
	}

	matches := otsdbh.formatRegexp.FindStringSubmatch(line)
	if len(matches) != 5 {
		logger.Error("this line does not follows the accepted pattern: "+line, loggerFields...)
		return
	}

	tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
	if len(tagMatches) == 0 {
		logger.Error("no parseable tags found in line: "+line, loggerFields...)
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
		logger.Error("no parseable timestamp found in line: "+line, loggerFields...)
		return
	}

	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		logger.Error("no parseable float number found in line: "+line, loggerFields...)
		return
	}

	point.Value = &value

	validatedPoint := &collector.Point{}

	err = pointCollector.MakePacket(validatedPoint, point, true)
	if err != nil {
		logger.Error("point validation failure in line: "+line, loggerFields...)
		return
	}

	pointCollector.HandlePacket(point, validatedPoint, true, "telnet-opentsdb", nil)
}
