package collector

import (
	"encoding/hex"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"

	"github.com/uol/gobol/hashing"
	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	stats *tsstats.StatsTS
)

const (
	cNumber          string = "number"
	cText            string = "text"
	cHandleJSONBytes string = "HandleJSONBytes"
)

// New - creates a new Collector
func New(
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	set *structs.Settings,
	keyspaceTTLMap map[int]string,
	ks *keyset.KeySet,
) (*Collector, error) {

	stats = sts

	collect := &Collector{
		cassandra:      cass,
		metaStorage:    metaStorage,
		validKey:       regexp.MustCompile(`^[0-9A-Za-z-\._\%\&\#\;\/]+$`),
		settings:       set,
		jobChannel:     make(chan workerData, set.MaxConcurrentPoints),
		keyspaceTTLMap: keyspaceTTLMap,
		keySet:         ks,
		logger:         logh.CreateContextualLogger(constants.StringsPKG, "collector"),
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
	keySet         *keyset.KeySet

	logger *logh.ContextualLogger
}

type workerData struct {
	validatedPoint *Point
	source         string
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
			statsPointsError(j.validatedPoint.Keyset, collect.getType(j.validatedPoint.Number), j.source, strconv.Itoa(j.validatedPoint.TTL))
			if logh.ErrorEnabled {
				collect.logger.Error().Str(constants.StringsFunc, "worker").Err(err)
			}
		} else {
			statsPoints(j.validatedPoint.Keyset, collect.getType(j.validatedPoint.Number), j.source, strconv.Itoa(j.validatedPoint.TTL))
		}
	}
}

// Stop - stops the UDP collector
func (collect *Collector) Stop() {
	collect.shutdown = true
}

func (collect *Collector) processPacket(point *Point) gobol.Error {

	start := time.Now()

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

	statsProcTime(point.Keyset, time.Since(start))

	return nil
}

// HandleJSONBytes - handles a point in byte format
func (collect *Collector) HandleJSONBytes(data []byte, source string, isNumber bool) (int, gobol.Error) {

	points, err := ParsePoints(cHandleJSONBytes, isNumber, data)
	if err != nil {
		return 0, err
	}

	err = points.Validate()
	if err != nil {
		return 0, err
	}

	for _, p := range points {

		vp, err := collect.MakePacket(p, isNumber)
		if err != nil {
			return 0, err
		}

		collect.HandlePacket(vp, source)
	}

	return len(points), nil
}

// HandlePacket - handles a point in struct format
func (collect *Collector) HandlePacket(vp *Point, source string) {

	collect.jobChannel <- workerData{
		validatedPoint: vp,
		source:         source,
	}
}

// GenerateID - generates the unique ID from a point
func (collect *Collector) GenerateID(rcvMsg *TSDBpoint) (string, error) {

	numParameters := (len(rcvMsg.Tags) * 2) + 1
	strParameters := make([]string, numParameters)
	strParameters[0] = rcvMsg.Metric

	i := 1
	for k, v := range rcvMsg.Tags {
		strParameters[i] = k
		i++
		strParameters[i] = v
		i++
	}

	sort.Strings(strParameters)

	parameters := make([]interface{}, numParameters)
	for i, v := range strParameters {
		parameters[i] = v
	}

	hash, err := hashing.GenerateSHAKE128(collect.settings.TSIDKeySize, parameters...)
	if err != nil {
		return constants.StringsEmpty, err
	}

	return hex.EncodeToString(hash), nil
}
