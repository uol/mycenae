package telnet

import (
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

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
func (otsdbh *OpenTSDBHandler) Handle(data *string, pointCollector *collector.Collector, logger *zap.Logger) {

	lines := strings.Split(*data, "\n")
	numLines := len(lines)

	for i := 0; i < numLines; i++ {

		if lines[i] == "" {
			continue
		}

		matches := otsdbh.formatRegexp.FindStringSubmatch(lines[i])
		if len(matches) != 5 {
			logger.Debug("this line does not follows the accepted pattern: " + lines[i])
			continue
		}

		tagMatches := otsdbh.tagsRegexp.FindAllStringSubmatch(matches[4], -1)
		if len(tagMatches) == 0 {
			logger.Debug("no parseable tags found in line: " + lines[i])
			continue
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
			logger.Debug("no parseable timestamp found in line: " + lines[i])
			continue
		}

		value, err := strconv.ParseFloat(matches[3], 64)
		if err != nil {
			logger.Debug("no parseable float number found in line: " + lines[i])
			continue
		}

		point.Value = &value

		validatedPoint := &collector.Point{}

		err = pointCollector.MakePacket(validatedPoint, point, true)
		if err != nil {
			logger.Debug("point validation failure in line: " + lines[i])
			continue
		}

		pointCollector.HandlePacket(point, validatedPoint, true, "telnet", nil)
	}
}
