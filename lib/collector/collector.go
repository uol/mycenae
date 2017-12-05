package collector

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/cache"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *logrus.Logger
	stats *tsstats.StatsTS
)

func New(
	log *structs.TsLog,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	es *rubber.Elastic,
	kc *cache.KeyspaceCache,
	set *structs.Settings,
) (*Collector, error) {

	d, err := time.ParseDuration(set.MetaSaveInterval)
	if err != nil {
		return nil, err
	}

	gblog = log.General
	stats = sts

	collect := &Collector{
		keyspaceCache: kc,
		persist:       persistence{cassandra: cass, esearch: es},
		validKey:      regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		settings:      set,
		concPoints:    make(chan struct{}, set.MaxConcurrentPoints),
		concBulk:      make(chan struct{}, set.MaxConcurrentBulks),
		metaChan:      make(chan Point, set.MetaBufferSize),
		metaPayload:   &bytes.Buffer{},
	}

	go collect.metaCoordinator(d)

	return collect, nil
}

type Collector struct {
	keyspaceCache *cache.KeyspaceCache
	persist       persistence
	validKey      *regexp.Regexp
	settings      *structs.Settings

	concPoints  chan struct{}
	concBulk    chan struct{}
	metaChan    chan Point
	metaPayload *bytes.Buffer

	receivedSinceLastProbe float64
	errorsSinceLastProbe   float64
	saving                 float64
	shutdown               bool
	saveMutex              sync.Mutex
	recvMutex              sync.Mutex
	errMutex               sync.Mutex
}

func (collect *Collector) CheckUDPbind() bool {
	lf := logrus.Fields{
		"struct": "CollectorV2",
		"func":   "CheckUDPbind",
	}

	port := ":" + collect.settings.UDPserverV2.Port

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		gblog.WithFields(lf).Error("addr:", err)
	}

	_, err = net.ListenUDP("udp", addr)
	if err != nil {
		gblog.WithFields(lf).Debug(err)
		return true
	}

	return false
}

func (collect *Collector) ReceivedErrorRatio() (ratio float64) {
	lf := logrus.Fields{
		"struct": "CollectorV2",
		"func":   "ReceivedErrorRatio",
	}

	if collect.receivedSinceLastProbe == 0 {
		ratio = 0
	} else {
		ratio = collect.errorsSinceLastProbe / collect.receivedSinceLastProbe
	}

	gblog.WithFields(lf).Debug(ratio)

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

func (collect *Collector) HandlePacket(rcvMsg TSDBpoint, number bool) gobol.Error {

	start := time.Now()

	go func() {
		collect.recvMutex.Lock()
		collect.receivedSinceLastProbe++
		collect.recvMutex.Unlock()
	}()

	packet := Point{}

	gerr := collect.makePacket(&packet, rcvMsg, number)
	if gerr != nil {
		return gerr
	}

	if number {
		if packet.Tuuid {
			gerr = collect.saveTUUIDvalue(packet)
		} else {
			gerr = collect.saveValue(packet)
		}
	} else {
		if packet.Tuuid {
			gerr = collect.saveTUUIDtext(packet)
		} else {
			gerr = collect.saveText(packet)
		}
	}

	if gerr != nil {
		collect.errMutex.Lock()
		collect.errorsSinceLastProbe++
		collect.errMutex.Unlock()
		return gerr
	}

	if len(collect.metaChan) < collect.settings.MetaBufferSize {
		go collect.saveMeta(packet)
	} else {
		gblog.WithFields(logrus.Fields{
			"func": "collector/HandlePacket",
		}).Warn("discarding point:", rcvMsg)
		statsLostMeta()
	}

	statsProcTime(packet.KsID, time.Since(start))
	return nil
}

func GenerateID(rcvMsg TSDBpoint) string {

	h := crc32.NewIEEE()

	if rcvMsg.Metric != "" {
		h.Write([]byte(rcvMsg.Metric))
	}

	mk := []string{}

	for k := range rcvMsg.Tags {
		if k != "ksid" && k != "ttl" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {

		h.Write([]byte(k))
		h.Write([]byte(rcvMsg.Tags[k]))

	}

	return fmt.Sprint(h.Sum32())
}

func (collect *Collector) CheckTSID(esType, id string) (bool, gobol.Error) {

	info := strings.Split(id, "|")

	respCode, gerr := collect.persist.HeadMetaFromES(info[0], esType, info[1])
	if gerr != nil {
		return false, gerr
	}
	if respCode != 200 {
		return false, nil
	}

	return true, nil
}
