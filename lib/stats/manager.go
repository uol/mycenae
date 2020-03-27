package stats

import (
	"fmt"
	"os"
	"time"

	"github.com/uol/hashing"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/utils"
	"github.com/uol/timeline"
)

//
// Manages the timeline instances.
// @author: rnojiri
//

// StorageType - the storage type constante
type StorageType string

const (
	// Normal - normal storage backend
	Normal StorageType = "normal"

	// Archive - archive storage backend
	Archive StorageType = "archive"
)

// BackendItem - one backend configuration
type BackendItem struct {
	timeline.Backend
	Type          StorageType
	CycleDuration utils.Duration
	CommonTags    map[string]string
}

// backendManager - internal type
type backendManager struct {
	manager    *timeline.Manager
	commonTags []interface{}
}

// Configuration - configuration
type Configuration struct {
	Backends             []BackendItem
	HashingAlgorithm     hashing.Algorithm
	HashSize             int
	TransportBufferSize  int
	SerializerBufferSize int
	BatchSendInterval    utils.Duration
	RequestTimeout       utils.Duration
	MaxReadTimeout       utils.Duration
	ReconnectionTimeout  utils.Duration
	DataTTL              utils.Duration
}

// TimelineManager - manages the configured number of timeline manager instances
type TimelineManager struct {
	backendMap    map[StorageType]backendManager
	logger        *logh.ContextualLogger
	hostName      string
	configuration *Configuration
	ready         bool
}

// New - creates a new instance
func New(configuration *Configuration) (*TimelineManager, error) {

	if len(configuration.Backends) == 0 {
		return nil, fmt.Errorf("no backends configured")
	}

	logger := logh.CreateContextualLogger(constants.StringsPKG, "stats")

	hostName, err := os.Hostname()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Msg("error getting host's name")
		}

		return nil, err
	}

	return &TimelineManager{
		backendMap:    nil,
		logger:        logger,
		hostName:      hostName,
		configuration: configuration,
	}, nil
}

// storageTypeNotFound - logs the storage type not found error
func (tm *TimelineManager) storageTypeNotFound(stype StorageType) {

	if logh.ErrorEnabled {
		tm.logger.Error().Msgf("storage type is not configured: %s", stype)
	}
}

// Flatten - performs a flatten operation
func (tm *TimelineManager) Flatten(caller string, stype StorageType, op timeline.FlatOperation, value float64, metric string, tags ...interface{}) {

	if !tm.ready {
		return
	}

	go func() {
		backend, ok := tm.backendMap[stype]
		if !ok {
			tm.storageTypeNotFound(stype)
			return
		}

		tags = append(tags, backend.commonTags...)

		err := backend.manager.FlattenOpenTSDB(op, value, time.Now().Unix(), metric, tags...)
		if err != nil {
			if logh.ErrorEnabled {
				ev := tm.logger.Error().Err(err)
				if len(caller) > 0 {
					ev = ev.Str(constants.StringsFunc, caller)
				}
				ev.Msg("flattening operation error")
			}
		}
	}()
}

// StoreDataToAccumulate - stores data to accumulate
func (tm *TimelineManager) StoreDataToAccumulate(caller string, stype StorageType, value float64, metric string, tags ...interface{}) (string, error) {

	backend, ok := tm.backendMap[stype]
	if !ok {
		return constants.StringsEmpty, fmt.Errorf("storage type is not configured")
	}

	tags = append(tags, backend.commonTags...)

	return backend.manager.StoreDataToAccumulateOpenTSDB(value, time.Now().Unix(), metric, tags...)
}

// Accumulate - performs a accumulate operation (check for ErrNotFound, raised when the hash is not stored)
func (tm *TimelineManager) Accumulate(caller string, stype StorageType, hash string) {

	if !tm.ready {
		return
	}

	go func() {
		backend, ok := tm.backendMap[stype]
		if !ok {
			tm.storageTypeNotFound(stype)
			return
		}

		err := backend.manager.IncrementAccumulatedData(hash)
		if err != nil {
			if logh.ErrorEnabled {
				ev := tm.logger.Error().Err(err)
				if len(caller) > 0 {
					ev = ev.Str(constants.StringsFunc, caller)
				}
				ev.Msg("accumulate operation error")
			}
		}
	}()
}

// Start - starts the timeline manager
func (tm *TimelineManager) Start() error {

	tc := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			SerializerBufferSize: tm.configuration.SerializerBufferSize,
			TransportBufferSize:  tm.configuration.TransportBufferSize,
			BatchSendInterval:    tm.configuration.BatchSendInterval.Duration,
			RequestTimeout:       tm.configuration.RequestTimeout.Duration,
		},
		MaxReadTimeout:      tm.configuration.MaxReadTimeout.Duration,
		ReconnectionTimeout: tm.configuration.ReconnectionTimeout.Duration,
	}

	t, err := timeline.NewOpenTSDBTransport(&tc)
	if err != nil {
		return err
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("opentsdb transport created")
	}

	tm.backendMap = map[StorageType]backendManager{}

	for i := 0; i < len(tm.configuration.Backends); i++ {

		b := timeline.Backend{
			Host: tm.configuration.Backends[i].Host,
			Port: tm.configuration.Backends[i].Port,
		}

		dtc := timeline.DataTransformerConf{
			CycleDuration:    tm.configuration.Backends[i].CycleDuration.Duration,
			HashSize:         tm.configuration.HashSize,
			HashingAlgorithm: tm.configuration.HashingAlgorithm,
		}

		f := timeline.NewFlattener(&dtc)

		ac := timeline.AccumulatorConf{
			DataTransformerConf: dtc,
			DataTTL:             tm.configuration.DataTTL.Duration,
		}

		a := timeline.NewAccumulator(&ac)

		manager, err := timeline.NewManager(t, f, a, &b)
		if err != nil {
			return err
		}

		tags := make([]interface{}, len(tm.configuration.Backends[i].CommonTags)*2)
		tagIndex := 0

		for k, v := range tm.configuration.Backends[i].CommonTags {
			tags[tagIndex] = k
			tagIndex++
			tags[tagIndex] = v
			tagIndex++
		}

		tm.backendMap[tm.configuration.Backends[i].Type] = backendManager{
			manager:    manager,
			commonTags: tags,
		}

		if logh.InfoEnabled {
			tm.logger.Info().Str("type", string(tm.configuration.Backends[i].Type)).Msgf("timeline manager created: %s:%d", b.Host, b.Port)
		}
	}

	for _, v := range tm.backendMap {
		v.manager.Start()
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was started")
	}

	tm.ready = true

	return nil
}

// Shutdown - shuts down the timeline manager
func (tm *TimelineManager) Shutdown() {

	for _, v := range tm.backendMap {
		v.manager.Shutdown()
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was shutdown")
	}
}
