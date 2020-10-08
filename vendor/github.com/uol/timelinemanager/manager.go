package timelinemanager

import (
	"fmt"
	"os"
	"time"

	"github.com/jinzhu/copier"
	"github.com/uol/logh"
	jsonSerializer "github.com/uol/serializer/json"
	"github.com/uol/serializer/serializer"
	"github.com/uol/timeline"
)

//
// Manages the timeline instances.
// @author: rnojiri
//

// backendManager - internal type
type backendManager struct {
	manager    *timeline.Manager
	commonTags []interface{}
	ttype      TransportType
}

// Instance - manages the configured number of timeline manager instances
type Instance struct {
	backendMap    map[StorageType]backendManager
	logger        *logh.ContextualLogger
	hostName      string
	configuration *Configuration
	ready         bool
}

type transportReference struct {
	transport timeline.Transport
	ttype     TransportType
}

type confReference struct {
	transportConf interface{}
	ttype         TransportType
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

func (tm *Instance) createSerializer(conf *TransportExt, bufferSize int) (serializer.Serializer, error) {

	if len(conf.Serializer) == 0 {
		conf.Serializer = JSONSerializer
	}

	if conf.Serializer == JSONSerializer {

		js := jsonSerializer.New(bufferSize)

		err := js.Add(
			cHTTPNumberFormat,
			jsonSerializer.NumberPoint{},
			cMetric,
			cValue,
			cTimestamp,
			cTags,
		)

		if err != nil {
			return nil, err
		}

		err = js.Add(
			cHTTPTextFormat,
			jsonSerializer.TextPoint{},
			cMetric,
			cText,
			cTimestamp,
			cTags,
		)

		if err != nil {
			return nil, err
		}

		if len(conf.JSONMappings) > 0 {
			for _, mapping := range conf.JSONMappings {
				err = js.Add(
					mapping.MappingName,
					mapping.Instance,
					mapping.Variables...,
				)

				if err != nil {
					return nil, err
				}
			}
		}

		return js, nil
	}

	if conf.Serializer == OpenTSDBSerializer {

		return jsonSerializer.New(bufferSize), nil
	}

	return nil, fmt.Errorf(`serializer named "%s" is not configured`, conf.Serializer)
}

func (tm *Instance) createHTTPTransport(conf *HTTPTransportConfigExt) (*transportReference, error) {

	s, err := tm.createSerializer(&conf.TransportExt, conf.SerializerBufferSize)
	if err != nil {
		return nil, err
	}

	httpTransport, err := timeline.NewHTTPTransport(&conf.HTTPTransportConfig, s)
	if err != nil {
		return nil, err
	}

	return &transportReference{
		transport: httpTransport,
		ttype:     HTTPTransport,
	}, nil
}

func (tm *Instance) createUDPTransport(conf *UDPTransportConfigExt) (*transportReference, error) {

	s, err := tm.createSerializer(&conf.TransportExt, conf.SerializerBufferSize)
	if err != nil {
		return nil, err
	}

	udpTransport, err := timeline.NewUDPTransport(&conf.UDPTransportConfig, s)
	if err != nil {
		return nil, err
	}

	return &transportReference{
		transport: udpTransport,
		ttype:     UDPTransport,
	}, nil
}

func (tm *Instance) createOpenTSDBTransport(conf *OpenTSDBTransportConfigExt) (*transportReference, error) {

	otsdbTransport, err := timeline.NewOpenTSDBTransport(&conf.OpenTSDBTransportConfig)
	if err != nil {
		return nil, err
	}

	return &transportReference{
		transport: otsdbTransport,
		ttype:     OpenTSDBTransport,
	}, nil
}

func (tm *Instance) createTransportRef(confName string, confMap map[string]confReference) (*transportReference, error) {

	confRef, ok := confMap[confName]
	if !ok {
		return nil, fmt.Errorf("transport configuration not found: %s", confName)
	}

	switch confRef.ttype {
	case HTTPTransport:
		httpConf := confRef.transportConf.(HTTPTransportConfigExt)
		return tm.createHTTPTransport(&httpConf)
	case OpenTSDBTransport:
		opentsdbConf := confRef.transportConf.(OpenTSDBTransportConfigExt)
		return tm.createOpenTSDBTransport(&opentsdbConf)
	case UDPTransport:
		udpConf := confRef.transportConf.(UDPTransportConfigExt)
		return tm.createUDPTransport(&udpConf)
	}

	return nil, fmt.Errorf("transport implementation not found: %s", confRef.ttype)
}

// Start - starts the timeline manager
func (tm *Instance) Start() error {

	transportConfMap := map[string]confReference{}

	for k, v := range tm.configuration.HTTPTransports {

		if _, exists := transportConfMap[k]; exists {
			return fmt.Errorf(`error creating http transport, name is duplicated: %s`, k)
		}

		confCopy := HTTPTransportConfigExt{}
		copier.Copy(&confCopy, &v)

		transportConfMap[k] = confReference{
			transportConf: confCopy,
			ttype:         HTTPTransport,
		}
	}

	for k, v := range tm.configuration.OpenTSDBTransports {

		if _, exists := transportConfMap[k]; exists {
			return fmt.Errorf(`error creating opentsdb transport, name is duplicated: %s`, k)
		}

		confCopy := OpenTSDBTransportConfigExt{}
		copier.Copy(&confCopy, &v)

		transportConfMap[k] = confReference{
			transportConf: confCopy,
			ttype:         OpenTSDBTransport,
		}
	}

	for k, v := range tm.configuration.UDPTransports {

		if _, exists := transportConfMap[k]; exists {
			return fmt.Errorf(`error creating udp transport, name is duplicated: %s`, k)
		}

		confCopy := UDPTransportConfigExt{}
		copier.Copy(&confCopy, &v)

		transportConfMap[k] = confReference{
			transportConf: confCopy,
			ttype:         UDPTransport,
		}
	}

	tm.backendMap = map[StorageType]backendManager{}

	for i := 0; i < len(tm.configuration.Backends); i++ {

		b := &tm.configuration.Backends[i].Backend

		dtc := timeline.DataTransformerConfig{
			CycleDuration:     tm.configuration.Backends[i].CycleDuration,
			HashSize:          tm.configuration.HashSize,
			HashingAlgorithm:  tm.configuration.HashingAlgorithm,
			PrintStackOnError: tm.configuration.PrintStackOnError,
		}

		f := timeline.NewFlattener(&dtc)
		a := timeline.NewAccumulator(&dtc)

		transportRef, err := tm.createTransportRef(tm.configuration.Backends[i].Transport, transportConfMap)
		if err != nil {
			return err
		}

		manager, err := timeline.NewManager(transportRef.transport, f, a, b, cLoggerStorage, string(tm.configuration.Backends[i].Storage))
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
			ttype:      transportRef.ttype,
		}

		err = manager.Start()
		if err != nil {
			return err
		}

		if logh.InfoEnabled {
			tm.logger.Info().Str(cType, string(transportRef.ttype)).Msgf("timeline manager created: %s:%d (%+v)", b.Host, b.Port, tags)
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
