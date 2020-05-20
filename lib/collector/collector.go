package collector

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/uol/mycenae/lib/stats"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"

	"github.com/uol/hashing"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

var (
	timelineManager *stats.TimelineManager
)

const (
	cNumber              string = "number"
	cText                string = "text"
	cFuncHandleJSONBytes string = "HandleJSONBytes"
)

// New - creates a new Collector
func New(
	tm *stats.TimelineManager,
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	set *structs.Settings,
	keyspaceTTLMap map[int]string,
	validation *validation.Service,
) (*Collector, error) {

	timelineManager = tm

	collect := &Collector{
		cassandra:      cass,
		metaStorage:    metaStorage,
		settings:       set,
		jobChannel:     make(chan workerData, set.MaxConcurrentPoints),
		keyspaceTTLMap: keyspaceTTLMap,
		logger:         logh.CreateContextualLogger(constants.StringsPKG, "collector"),
		validation:     validation,
	}

	for i := 0; i < set.MaxConcurrentPoints; i++ {
		go collect.worker(i, collect.jobChannel)
	}

	return collect, nil
}

// Collector - implements a point collector structure
type Collector struct {
	cassandra   *gocql.Session
	metaStorage *metadata.Storage
	validKey    *regexp.Regexp
	settings    *structs.Settings

	shutdown       bool
	jobChannel     chan workerData
	keyspaceTTLMap map[int]string

	validation *validation.Service
	logger     *logh.ContextualLogger
}

type workerData struct {
	validatedPoint *Point
	source         *constants.SourceType
}

func (collect *Collector) getType(number bool) string {
	if number {
		return cNumber
	}
	return cText
}

func (collect *Collector) worker(id int, jobChannel <-chan workerData) {

	for j := range jobChannel {

		err := collect.processPacket(j.validatedPoint)
		if err != nil {
			statsPointsError(j.validatedPoint.Message.Keyset, collect.getType(j.validatedPoint.Number), j.source, j.validatedPoint.Message.TTL)
			if logh.ErrorEnabled {
				collect.logger.Error().Str(constants.StringsFunc, "worker").Err(err).Send()
			}
		} else {
			statsPoints(j.validatedPoint.Message.Keyset, collect.getType(j.validatedPoint.Number), j.source, j.validatedPoint.Message.TTL)
		}
	}
}

// Stop - stops the UDP collector
func (collect *Collector) Stop() {
	collect.shutdown = true
}

func (collect *Collector) processPacket(point *Point) gobol.Error {

	start := time.Now()

	pastTime := start.Unix() - point.Message.Timestamp

	if pastTime >= collect.settings.DelayedMetricsThreshold {
		statsDelayedMetrics(point.Message.Keyset, pastTime)
	}

	var gerr gobol.Error

	if point.Number {
		gerr = collect.saveValue(point)
	} else {
		gerr = collect.saveText(point)
	}

	if gerr != nil {
		return gerr
	}

	gerr = collect.saveMeta(point)
	if gerr != nil {
		return gerr
	}

	statsProcTime(point.Message.Keyset, time.Since(start))

	return nil
}

// HandleJSONBytes - handles a point in byte format
func (collect *Collector) HandleJSONBytes(data []byte, sourceType *constants.SourceType, ip string, isNumber bool) (int, gobol.Error) {

	points := structs.TSDBpoints{}
	gerrs := []gobol.Error{}

	keyset := collect.ParsePoints(cFuncHandleJSONBytes, isNumber, data, &points, &gerrs)
	numErrs := len(gerrs)
	if numErrs > 0 {
		for _, gerr := range gerrs {
			collect.validation.StatsValidationError(cFuncHandleJSONBytes, keyset, ip, sourceType, gerr)
		}

		if numErrs == 1 {
			return 0, gerrs[0]
		}

		return 0, errMultipleErrors(cFuncHandleJSONBytes, gerrs)
	}

	if len(points) == 0 {
		return 0, nil
	}

	for _, p := range points {

		vp, err := collect.MakePacket(p, isNumber)
		if err != nil {
			return 0, err
		}

		collect.HandlePacket(vp, sourceType)
	}

	return len(points), nil
}

const cTextTSIDFormat string = "T%v"

// MakePacket - validates a point and fills the packet
func (collect *Collector) MakePacket(rcvMsg *structs.TSDBpoint, number bool) (*Point, gobol.Error) {

	packet := &Point{}

	var err error
	packet.Number = number
	packet.Message = rcvMsg
	packet.ID, packet.HashID, err = collect.GenerateID(rcvMsg)

	if err != nil {
		return nil, errInternalServerError("makePacket", "error creating the tsid hash", err)
	}

	if !number {
		packet.ID = fmt.Sprintf(cTextTSIDFormat, packet.ID)
	}

	return packet, nil
}

// HandlePacket - handles a point in struct format
func (collect *Collector) HandlePacket(vp *Point, source *constants.SourceType) {

	collect.jobChannel <- workerData{
		validatedPoint: vp,
		source:         source,
	}
}

// GenerateID - generates the unique ID from a point
func (collect *Collector) GenerateID(rcvMsg *structs.TSDBpoint) (string, []byte, error) {

	numParameters := (len(rcvMsg.Tags) * 2) + 1
	strParameters := make([]string, numParameters)
	strParameters[0] = rcvMsg.Metric

	i := 1
	for _, tag := range rcvMsg.Tags {
		strParameters[i] = tag.Name
		i++
		strParameters[i] = tag.Value
		i++
	}

	sort.Strings(strParameters)

	parameters := make([]interface{}, numParameters)
	for i, v := range strParameters {
		parameters[i] = v
	}

	hash, err := hashing.GenerateSHAKE128(collect.settings.TSIDKeySize, parameters...)
	if err != nil {
		return constants.StringsEmpty, nil, err
	}

	return hex.EncodeToString(hash), hash, nil
}
