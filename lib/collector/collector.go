package collector

import (
	"fmt"
	"hash/crc32"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

// New - creates a new Collector
func New(
	log *structs.Loggers,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	set *structs.Settings,
	keyspaceTTLMap map[int]string,
	ks *keyset.KeySet,
) (*Collector, error) {

	d, err := time.ParseDuration(set.MetaSaveInterval)
	if err != nil {
		return nil, err
	}

	gblog = log.General
	stats = sts

	collect := &Collector{
		persist:        persistence{cassandra: cass, metaStorage: metaStorage},
		validKey:       regexp.MustCompile(`^[0-9A-Za-z-\._\%\&\#\;\/]+$`),
		settings:       set,
		concBulk:       make(chan struct{}, set.MaxConcurrentBulks),
		metaChan:       make(chan *Point, set.MetaBufferSize),
		metadataMap:    sync.Map{},
		jobChannel:     make(chan workerData, set.MaxConcurrentPoints),
		keyspaceTTLMap: keyspaceTTLMap,
		keySet:         ks,
	}

	for i := 0; i < set.MaxConcurrentPoints; i++ {
		go collect.worker(i, collect.jobChannel)
	}

	go collect.metaCoordinator(d)

	return collect, nil
}

// Collector - implements a point collector structure
type Collector struct {
	persist  persistence
	validKey *regexp.Regexp
	settings *structs.Settings

	concBulk    chan struct{}
	metaChan    chan *Point
	metadataMap sync.Map

	shutdown       bool
	jobChannel     chan workerData
	keyspaceTTLMap map[int]string
	keySet         *keyset.KeySet
}

type workerData struct {
	validatedPoint *Point
	source         string
}

func (collect *Collector) getType(number bool) string {
	if number {
		return "number"
	}
	return "text"
}

func (collect *Collector) worker(id int, jobChannel <-chan workerData) {

	for j := range jobChannel {

		err := collect.processPacket(j.validatedPoint)
		if err != nil {
			statsPointsError(j.validatedPoint.Keyset, collect.getType(j.validatedPoint.Number), j.source, strconv.Itoa(j.validatedPoint.TTL))
			gblog.Error(err.Error(), zap.String("package", "collector"), zap.String("func", "worker"))
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

	if len(collect.metaChan) < collect.settings.MetaBufferSize {

		collect.saveMeta(point)

	} else {

		lf := []zapcore.Field{
			zap.String("package", "collector/collector"),
			zap.String("func", "processPacket"),
		}

		gblog.Warn("discarding point, no space in the meta buffer", lf...)

		statsLostMeta()
	}

	statsProcTime(point.Keyset, time.Since(start))

	return nil
}

// HandleJSONBytes - handles a point in byte format
func (collect *Collector) HandleJSONBytes(data []byte, source string, isNumber bool) (int, gobol.Error) {

	points, err := ParsePoints("HandleJSONBytes", isNumber, data)
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
func GenerateID(rcvMsg *TSDBpoint) string {

	h := crc32.NewIEEE()

	if rcvMsg.Metric != "" {
		h.Write([]byte(rcvMsg.Metric))
	}

	mk := []string{}

	for k := range rcvMsg.Tags {
		mk = append(mk, k)
	}

	sort.Strings(mk)

	for _, k := range mk {

		h.Write([]byte(k))
		h.Write([]byte(rcvMsg.Tags[k]))

	}

	return fmt.Sprint(h.Sum32())
}
