package plot

import (
	"errors"
	"unsafe"

	"github.com/uol/logh"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
	tlmanager "github.com/uol/timelinemanager"
)

type persistence struct {
	metaStorage                   *metadata.Storage
	cassandra                     *gocql.Session
	constPartBytesFromNumberPoint uintptr
	constPartBytesFromTextPoint   uintptr
	stringSize                    uintptr
	maxBytesErr                   error
	timelineManager               *tlmanager.Instance
	unlimitedBytesKeysetWhiteList map[string]bool
	logger                        *logh.ContextualLogger
	clusteringOrder               constants.ClusteringOrder
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
	timelineManager *tlmanager.Instance,
	clusteringOrder constants.ClusteringOrder,
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
			timelineManager:               timelineManager,
			cassandra:                     cass,
			metaStorage:                   metaStorage,
			stringSize:                    stringSize,
			constPartBytesFromNumberPoint: unsafe.Sizeof(Pnt{}),                  //removing the tsid part because it's a string
			constPartBytesFromTextPoint:   unsafe.Sizeof(TextPnt{}) - stringSize, //removing the tsid and value because they are all strings
			maxBytesErr:                   errors.New("payload too large"),
			unlimitedBytesKeysetWhiteList: unlimitedBytesKeysetWhiteMap,
			logger:                        logh.CreateContextualLogger(constants.StringsPKG, "plot/persistence"),
			clusteringOrder:               clusteringOrder,
		},
		keyspaceTTLMap:    keyspaceTTLMap,
		defaultTTL:        defaultTTL,
		defaultMaxResults: defaultMaxResults,
		maxBytesLimit:     maxBytesLimit,
		logger:            logh.CreateContextualLogger(constants.StringsPKG, "plot"),
		timelineManager:   timelineManager,
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
	timelineManager     *tlmanager.Instance
	logger              *logh.ContextualLogger
}

// getStringSize - calculates the string size
func (persist *persistence) getStringSize(text string) uintptr {

	return (uintptr(len(text)) + persist.stringSize)
}
