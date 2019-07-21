package plot

import (
	"errors"
	"unsafe"

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
	metaStorage                   *metadata.Storage
	cassandra                     *gocql.Session
	constPartBytesFromNumberPoint uintptr
	constPartBytesFromTextPoint   uintptr
	stringSize                    uintptr
	maxBytesErr                   error
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
	maxBytesLimit uint32,

) (*Plot, gobol.Error) {

	gblog = gbl
	stats = sts

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	stringSize := unsafe.Sizeof("")

	return &Plot{
		MaxTimeseries:       maxTimeseries,
		LogQueryTSThreshold: logQueryTSthreshold,
		persist: persistence{
			cassandra:                     cass,
			metaStorage:                   metaStorage,
			stringSize:                    stringSize,
			constPartBytesFromNumberPoint: unsafe.Sizeof(Pnt{}),                  //removing the tsid part because it's a string
			constPartBytesFromTextPoint:   unsafe.Sizeof(TextPnt{}) - stringSize, //removing the tsid and value because they are all strings
			maxBytesErr:                   errors.New("payload too large"),
		},
		keyspaceTTLMap:    keyspaceTTLMap,
		defaultTTL:        defaultTTL,
		defaultMaxResults: defaultMaxResults,
		maxBytesLimit:     maxBytesLimit,
	}, nil
}

type Plot struct {
	MaxTimeseries       int
	LogQueryTSThreshold int
	persist             persistence
	keyspaceTTLMap      map[int]string
	defaultTTL          int
	defaultMaxResults   int
	maxBytesLimit       uint32
}

// getStringSize - calculates the string size
func (persist *persistence) getStringSize(text string) uintptr {

	return (uintptr(len(text)) + persist.stringSize)
}
