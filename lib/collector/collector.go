package collector

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"regexp"
	"sort"
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

func New(
	log *structs.TsLog,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	set *structs.Settings,
	keyspaceTTLMap map[uint8]string,
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
		metaChan:       make(chan Point, set.MetaBufferSize),
		metadataMap:    map[string][]metadata.Metadata{},
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

type Collector struct {
	persist  persistence
	validKey *regexp.Regexp
	settings *structs.Settings

	concBulk    chan struct{}
	metaChan    chan Point
	metadataMap map[string][]metadata.Metadata

	receivedSinceLastProbe float64
	errorsSinceLastProbe   float64
	saving                 float64
	shutdown               bool
	saveMutex              sync.Mutex
	recvMutex              sync.Mutex
	errMutex               sync.Mutex
	jobChannel             chan workerData
	keyspaceTTLMap         map[uint8]string
	keySet                 *keyset.KeySet
}

type workerData struct {
	point          TSDBpoint
	validatedPoint *Point
	number         bool
	source         string
	logFields      map[string]string
}

func (collect *Collector) getType(number bool) string {
	if number {
		return "number"
	}
	return "text"
}

func (collect *Collector) worker(id int, jobChannel <-chan workerData) {

	for j := range jobChannel {
		err := collect.processPacket(j.point, j.validatedPoint, j.number)
		if err != nil {
			statsPointsError(j.point.Tags["ksid"], collect.getType(j.number), j.source, j.point.Tags["ttl"])
			lf := []zapcore.Field{
				zap.String("package", "collector"),
				zap.String("func", "worker"),
			}
			if j.logFields != nil && len(j.logFields) > 0 {
				for k, v := range j.logFields {
					lf = append(lf, zap.String(k, v))
				}
			}
			jsonStr, err := json.Marshal(j.point)
			if err != nil {
				gblog.Error("point lost (error converting to string)...", lf...)
			} else {
				gblog.Error(fmt.Sprintf("point lost: %s", jsonStr), lf...)
			}
		} else {
			statsPoints(j.point.Tags["ksid"], collect.getType(j.number), j.source, j.point.Tags["ttl"])
		}
	}
}

func (collect *Collector) ReceivedErrorRatio() (ratio float64) {
	lf := []zapcore.Field{
		zap.String("struct", "Collector"),
		zap.String("func", "ReceivedErrorRatio"),
	}
	if collect.receivedSinceLastProbe == 0 {
		ratio = 0
	} else {
		ratio = collect.errorsSinceLastProbe / collect.receivedSinceLastProbe
	}

	gblog.Debug(fmt.Sprintf("%f", ratio), lf...)

	collect.recvMutex.Lock()
	collect.receivedSinceLastProbe = 0
	collect.recvMutex.Unlock()
	collect.errMutex.Lock()
	collect.errorsSinceLastProbe = 0
	collect.errMutex.Unlock()

	return
}

func (collect *Collector) Stop() {
	collect.shutdown = true
	for {
		if collect.saving <= 0 {
			return
		}
	}
}

func (collect *Collector) processPacket(rcvMsg TSDBpoint, point *Point, number bool) gobol.Error {

	start := time.Now()

	var gerr gobol.Error
	var packet Point

	if point == nil {
		packet = Point{}
		gerr := collect.makePacket(&packet, rcvMsg, number)
		if gerr != nil {
			return gerr
		}
	} else {
		packet = *point
	}

	go func() {
		collect.recvMutex.Lock()
		collect.receivedSinceLastProbe++
		collect.recvMutex.Unlock()
	}()

	if number {
		gerr = collect.saveValue(&packet)
	} else {
		gerr = collect.saveText(&packet)
	}

	if gerr != nil {
		collect.errMutex.Lock()
		collect.errorsSinceLastProbe++
		collect.errMutex.Unlock()
		return gerr
	}

	if len(collect.metaChan) < collect.settings.MetaBufferSize {
		collect.saveMeta(packet)
	} else {
		lf := []zapcore.Field{
			zap.String("package", "collector/collector"),
			zap.String("func", "processPacket"),
		}

		jsonStr, err := json.Marshal(rcvMsg)
		if err != nil {
			gblog.Error("point discarded (error converting to string)...", lf...)
		} else {
			gblog.Warn(fmt.Sprintf("discarding point: %s", jsonStr), lf...)
		}

		statsLostMeta()
	}

	statsProcTime(packet.Keyset, time.Since(start))

	return nil
}

func (collect *Collector) HandlePacket(rcvMsg TSDBpoint, vp *Point, number bool, source string, logFields map[string]string) {

	collect.jobChannel <- workerData{
		point:          rcvMsg,
		validatedPoint: vp,
		number:         number,
		source:         source,
		logFields:      logFields,
	}
}

func GenerateID(rcvMsg TSDBpoint) string {

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
