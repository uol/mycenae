package timelinemanager

import (
	"fmt"
	"os"
	"time"

	"github.com/uol/funks"
	"github.com/uol/hashing"
	"github.com/uol/logh"
	jsonSerializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

//
// Manages the timeline instances.
// @author: rnojiri
//

// StorageType - the storage type constant
type StorageType string

// TransportType - the transport type constant
type TransportType string

const (
	// Normal - normal storage backend
	Normal StorageType = "normal"

	// Archive - archive storage backend
	Archive StorageType = "archive"

	// HTTP - http transport type
	HTTP TransportType = "http"

	// OpenTSDB - opentsdb transport type
	OpenTSDB TransportType = "opentsdb"

	cFunction  string = "func"
	cType      string = "type"
	cOperation string = "operation"
	cHost      string = "host"

	cHTTPNumberFormat string = "httpNumberFormat"
	cHTTPTextFormat   string = "httpTextFormat"
)

// ErrStorageNotFound - raised when a storage type was not found
var ErrStorageNotFound error = fmt.Errorf("storage type not found")

// ErrTransportNotSupported - raised when a transport is not supported for the specified storage
var ErrTransportNotSupported error = fmt.Errorf("transport not supported")

// BackendItem - one backend configuration
type BackendItem struct {
	timeline.Backend
	Storage       StorageType
	Type          TransportType
	CycleDuration funks.Duration
	AddHostTag    bool
	CommonTags    map[string]string
}

// backendManager - internal type
type backendManager struct {
	manager    *timeline.Manager
	commonTags []interface{}
	ttype      TransportType
}

// Configuration - configuration
type Configuration struct {
	Backends         []BackendItem
	HashingAlgorithm hashing.Algorithm
	HashSize         int
	DataTTL          funks.Duration
	timeline.DefaultTransportConfiguration
	OpenTSDBTransport *timeline.OpenTSDBTransportConfig
	HTTPTransport     *timeline.HTTPTransportConfig
}

// Validate - validates the configuration
func (c *Configuration) Validate() error {

	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}

	var hasOpenTSDB, hasHTTP bool

	if hasOpenTSDB = c.OpenTSDBTransport != nil; hasOpenTSDB {
		c.OpenTSDBTransport.DefaultTransportConfiguration = c.DefaultTransportConfiguration
	}

	if hasHTTP = c.HTTPTransport != nil; hasHTTP {
		c.HTTPTransport.DefaultTransportConfiguration = c.DefaultTransportConfiguration
	}

	if !hasOpenTSDB && !hasHTTP {
		return fmt.Errorf("no transports configured")
	}

	return nil
}

// Instance - manages the configured number of timeline manager instances
type Instance struct {
	backendMap    map[StorageType]backendManager
	logger        *logh.ContextualLogger
	hostName      string
	configuration *Configuration
	ready         bool
}

// New - creates a new instance
func New(configuration *Configuration) (*Instance, error) {

	logger := logh.CreateContextualLogger("pkg", "stats")

	if configuration == nil {
		return nil, fmt.Errorf("configuration is null")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	hostName, err := os.Hostname()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Msg("error getting host's name")
		}

		return nil, err
	}

	return &Instance{
		logger:        logger,
		hostName:      hostName,
		configuration: configuration,
	}, nil
}

// storageTypeNotFound - logs the storage type not found error
func (tm *Instance) storageTypeNotFound(function string, stype StorageType) error {

	if logh.ErrorEnabled {
		ev := tm.logger.Error()
		if len(function) > 0 {
			ev = ev.Str(cFunction, function)
		}

		ev.Msgf("storage type is not configured: %s", stype)
	}

	return ErrStorageNotFound
}

// Start - starts the timeline manager
func (tm *Instance) Start() error {

	tm.backendMap = map[StorageType]backendManager{}

	for i := 0; i < len(tm.configuration.Backends); i++ {

		b := timeline.Backend{
			Host: tm.configuration.Backends[i].Host,
			Port: tm.configuration.Backends[i].Port,
		}

		name := fmt.Sprintf(
			"%s-%s:%d",
			tm.configuration.Backends[i].Storage,
			b.Host,
			b.Port,
		)

		dtc := timeline.DataTransformerConf{
			CycleDuration:    tm.configuration.Backends[i].CycleDuration,
			HashSize:         tm.configuration.HashSize,
			HashingAlgorithm: tm.configuration.HashingAlgorithm,
			Name:             name,
		}

		f := timeline.NewFlattener(&dtc)
		a := timeline.NewAccumulator(&dtc)

		var manager *timeline.Manager
		var err error

		if tm.configuration.Backends[i].Type == OpenTSDB {

			conf := *tm.configuration.OpenTSDBTransport
			conf.Name = name

			opentsdbTransport, err := timeline.NewOpenTSDBTransport(&conf)
			if err != nil {
				return err
			}

			manager, err = timeline.NewManager(opentsdbTransport, f, a, &b)

		} else if tm.configuration.Backends[i].Type == HTTP {

			conf := *tm.configuration.HTTPTransport
			conf.Name = name

			httpTransport, err := timeline.NewHTTPTransport(&conf)
			if err != nil {
				return err
			}

			httpTransport.AddJSONMapping(
				cHTTPNumberFormat,
				jsonSerializer.NumberPoint{},
				cMetric,
				cValue,
				cTimestamp,
				cTags,
			)

			httpTransport.AddJSONMapping(
				cHTTPTextFormat,
				jsonSerializer.TextPoint{},
				cMetric,
				cText,
				cTimestamp,
				cTags,
			)

			manager, err = timeline.NewManager(httpTransport, f, a, &b)

		} else {

			err = fmt.Errorf("transport type %s is undefined", tm.configuration.Backends[i].Type)
		}

		if err != nil {
			return err
		}

		numHostTags := 0
		if tm.configuration.Backends[i].AddHostTag {
			numHostTags = 2
		}

		tags := make([]interface{}, numHostTags+len(tm.configuration.Backends[i].CommonTags)*2)

		tagIndex := 0
		for k, v := range tm.configuration.Backends[i].CommonTags {
			tags[tagIndex] = k
			tagIndex++
			tags[tagIndex] = v
			tagIndex++
		}

		if tm.configuration.Backends[i].AddHostTag {
			tags[tagIndex] = cHost
			tagIndex++
			tags[tagIndex] = tm.hostName
		}

		if _, exists := tm.backendMap[tm.configuration.Backends[i].Storage]; exists {
			return fmt.Errorf(`backend named "%s" is registered more than one time`, tm.configuration.Backends[i].Storage)
		}

		tm.backendMap[tm.configuration.Backends[i].Storage] = backendManager{
			manager:    manager,
			commonTags: tags,
			ttype:      tm.configuration.Backends[i].Type,
		}

		err = manager.Start()
		if err != nil {
			return err
		}

		if logh.InfoEnabled {
			tm.logger.Info().Str(cType, string(tm.configuration.Backends[i].Type)).Msgf("timeline manager created: %s:%d (%+v)", b.Host, b.Port, tags)
		}
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was started")
	}

	tm.ready = true

	return nil
}

// Shutdown - shuts down the timeline manager
func (tm *Instance) Shutdown() {

	for _, v := range tm.backendMap {
		v.manager.Shutdown()
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was shutdown")
	}
}

// GetConfiguredDataTTL - returns the configured data ttl
func (tm *Instance) GetConfiguredDataTTL() time.Duration {
	return tm.configuration.DataTTL.Duration
}
