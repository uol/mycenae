package plot

import (
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/cache"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

type persistence struct {
	metaStorage *metadata.Storage
	cassandra   *gocql.Session
}

func New(
	gbl *zap.Logger,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	ks *cache.KeyspaceCache,
	maxTimeseries int,
	maxConcurrentTimeseries int,
	maxConcurrentReads int,
	logQueryTSthreshold int,
	keyspaceTTLMap map[uint8]string,
	keySet *keyset.KeySet,
	defaultTTL uint8,
	defaultMaxResults int,

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
		MaxTimeseries:     maxTimeseries,
		LogQueryThreshold: logQueryTSthreshold,
		keyspaceCache:     ks,
		persist:           persistence{cassandra: cass, metaStorage: metaStorage},
		concTimeseries:    make(chan struct{}, maxConcurrentTimeseries),
		concReads:         make(chan struct{}, maxConcurrentReads),
		keyspaceTTLMap:    keyspaceTTLMap,
		keySet:            keySet,
		defaultTTL:        defaultTTL,
		defaultMaxResults: defaultMaxResults,
	}, nil
}

type Plot struct {
	MaxTimeseries     int
	LogQueryThreshold int
	keyspaceCache     *cache.KeyspaceCache
	persist           persistence
	concTimeseries    chan struct{}
	concReads         chan struct{}
	keyspaceTTLMap    map[uint8]string
	keySet            *keyset.KeySet
	defaultTTL        uint8
	defaultMaxResults int
}
