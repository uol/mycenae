package plot

import (
	"errors"
	"unsafe"

	"github.com/uol/logh"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tsstats"
)

type persistence struct {
	metaStorage                   *metadata.Storage
	cassandra                     *gocql.Session
	constPartBytesFromNumberPoint uintptr
	constPartBytesFromTextPoint   uintptr
	stringSize                    uintptr
	maxBytesErr                   error
	stats                         *tsstats.StatsTS
	unlimitedBytesKeysetWhiteList map[string]bool
	logger                        *logh.ContextualLogger
}

func New(
	cass *gocql.Session,
	metaStorage *metadata.Storage,
	maxTimeseries int,
	logQueryTSthreshold int,
	keyspaceTTLMap map[int]string,
	defaultTTL int,
	defaultMaxResults int,
	maxBytesLimit uint32,
	unlimitedBytesKeysetWhiteList []string,
	stats *tsstats.StatsTS,
) (*Plot, gobol.Error) {

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	stringSize := unsafe.Sizeof(constants.StringsEmpty)

	unlimitedBytesKeysetWhiteMap := map[string]bool{}
	for _, keyset := range unlimitedBytesKeysetWhiteList {
		unlimitedBytesKeysetWhiteMap[keyset] = true
	}

	return &Plot{
		MaxTimeseries:       maxTimeseries,
		LogQueryTSThreshold: logQueryTSthreshold,
		persist: &persistence{
			stats:                         stats,
			cassandra:                     cass,
			metaStorage:                   metaStorage,
			stringSize:                    stringSize,
			constPartBytesFromNumberPoint: unsafe.Sizeof(Pnt{}),                  //removing the tsid part because it's a string
			constPartBytesFromTextPoint:   unsafe.Sizeof(TextPnt{}) - stringSize, //removing the tsid and value because they are all strings
			maxBytesErr:                   errors.New("payload too large"),
			unlimitedBytesKeysetWhiteList: unlimitedBytesKeysetWhiteMap,
			logger:                        logh.CreateContextualLogger(constants.StringsPKG, "plot/persistence"),
		},
		keyspaceTTLMap:    keyspaceTTLMap,
		defaultTTL:        defaultTTL,
		defaultMaxResults: defaultMaxResults,
		maxBytesLimit:     maxBytesLimit,
		logger:            logh.CreateContextualLogger(constants.StringsPKG, "plot"),
		stats:             stats,
	}, nil
}

type Plot struct {
	MaxTimeseries       int
	LogQueryTSThreshold int
	persist             *persistence
	keyspaceTTLMap      map[int]string
	defaultTTL          int
	defaultMaxResults   int
	maxBytesLimit       uint32
	stats               *tsstats.StatsTS
	logger              *logh.ContextualLogger
}

// getStringSize - calculates the string size
func (persist *persistence) getStringSize(text string) uintptr {

	return (uintptr(len(text)) + persist.stringSize)
}
