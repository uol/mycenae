package plot

import (
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/cache"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func New(
	gbl *zap.Logger,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	es *rubber.Elastic,
	ks *cache.KeyspaceCache,
	esIndex string,
	maxTimeseries int,
	maxConcurrentTimeseries int,
	maxConcurrentReads int,
	logQueryTSthreshold int,
	keyspaceTTLMap map[uint8]string,
	keySet *keyset.KeySet,
	defaultTTL uint8,

) (*Plot, gobol.Error) {

	gblog = gbl
	stats = sts

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if maxConcurrentReads < 1 {
		return nil, errInit("MaxConcurrentReads needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	if maxConcurrentTimeseries > maxConcurrentReads {
		return nil, errInit("maxConcurrentTimeseries cannot be bigger than maxConcurrentReads")
	}

	return &Plot{
		esIndex:           esIndex,
		MaxTimeseries:     maxTimeseries,
		LogQueryThreshold: logQueryTSthreshold,
		keyspaceCache:     ks,
		persist:           persistence{cassandra: cass, esTs: es},
		concTimeseries:    make(chan struct{}, maxConcurrentTimeseries),
		concReads:         make(chan struct{}, maxConcurrentReads),
		keyspaceTTLMap:    keyspaceTTLMap,
		keySet:            keySet,
		defaultTTL:        defaultTTL,
	}, nil
}

type Plot struct {
	esIndex           string
	MaxTimeseries     int
	LogQueryThreshold int
	keyspaceCache     *cache.KeyspaceCache
	persist           persistence
	concTimeseries    chan struct{}
	concReads         chan struct{}
	keyspaceTTLMap    map[uint8]string
	keySet            *keyset.KeySet
	defaultTTL        uint8
	maxSuggestions    int
}
