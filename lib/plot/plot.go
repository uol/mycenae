package plot

import (
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"

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
	maxTimeseries int,
	logQueryTSthreshold int,
	keyspaceTTLMap map[int]string,
	defaultTTL int,
	defaultMaxResults int,

) (*Plot, gobol.Error) {

	gblog = gbl
	stats = sts

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	return &Plot{
		MaxTimeseries:     maxTimeseries,
		LogQueryThreshold: logQueryTSthreshold,
		persist:           persistence{cassandra: cass, metaStorage: metaStorage},
		keyspaceTTLMap:    keyspaceTTLMap,
		defaultTTL:        defaultTTL,
		defaultMaxResults: defaultMaxResults,
	}, nil
}

type Plot struct {
	MaxTimeseries     int
	LogQueryThreshold int
	persist           persistence
	keyspaceTTLMap    map[int]string
	defaultTTL        int
	defaultMaxResults int
}
