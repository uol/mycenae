package timeline

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/logh"
	"github.com/uol/scheduler"
)

/**
* This struct is similar to the flattener count operation, but it stores references to do large counts consuming less memory.
* @author rnojiri
**/

const (
	// AccumulatorName - the name
	AccumulatorName string = "accumulator"
)

var (
	// ErrNotStored - thrown when a hash was not stored
	ErrNotStored error = fmt.Errorf("hash is not stored")
)

// accumulatedData - an accumulated point
type accumulatedData struct {
	count      uint64
	hash       string
	data       interface{}
	lastUpdate time.Time
	ttl        time.Duration
	pointMap   *sync.Map
	ttlManager *scheduler.Manager
	logger     *logh.ContextualLogger
}

// GetHash - returns the hash
func (ad *accumulatedData) GetHash() string {
	return ad.hash
}

// Execute - implements the Job interface
func (ad *accumulatedData) Execute() {

	if time.Now().Sub(ad.lastUpdate) > ad.ttl {

		ad.pointMap.Delete(ad.hash)
		ad.ttlManager.RemoveTask(ad.hash)

		if logh.InfoEnabled {
			ad.logger.Info().Str("hash", ad.hash).Msgf("data removed")
		}

	} else if logh.DebugEnabled {
		ad.logger.Debug().Str("hash", ad.hash).Msgf("ttl still valid")
	}
}

// Clone - does a struct copy
func (ad *accumulatedData) Clone() interface{} {

	// no need
	return nil
}

// ReleaseResources - release this item resources
func (ad *accumulatedData) ReleaseResources() {

	// no need
}

// Accumulator - the struct
type Accumulator struct {
	dataProcessorCore
	ttlManager *scheduler.Manager
}

// NewAccumulator - creates a new instance
func NewAccumulator(configuration *DataTransformerConfig) *Accumulator {

	configuration.isSHAKE = isShakeAlgorithm(configuration.HashingAlgorithm)

	a := &Accumulator{
		ttlManager: scheduler.New(),
		dataProcessorCore: dataProcessorCore{
			configuration: configuration,
			pointMap:      sync.Map{},
		},
	}

	a.parent = a

	return a
}

// BuildContextualLogger - build the contextual logger using more info
func (a *Accumulator) BuildContextualLogger(path ...string) {

	logContext := []string{"pkg", "timeline/accumulator"}

	if len(path) > 0 {
		logContext = append(logContext, path...)
	}

	a.loggers = logh.CreateContextualLogger(logContext...)
}

// ProcessMapEntry - sends the data to the transport
func (a *Accumulator) ProcessMapEntry(entry dataProcessorEntry) bool {

	data := entry.(*accumulatedData)

	if data.count > 0 {
		item, err := a.transport.AccumulatedDataToDataChannelItem(data)
		if err != nil {
			if logh.ErrorEnabled {
				ev := a.loggers.Error()
				if a.dataProcessorCore.configuration.PrintStackOnError {
					ev = ev.Caller()
				}
				ev.Err(err).Msg(err.Error())
			}
		}

		a.transport.DataChannel(item)

		atomic.StoreUint64(&data.count, 0)
		data.lastUpdate = time.Now()
	}

	return false
}

// Add - adds one more to the reference
func (a *Accumulator) Add(hash string) error {

	item, ok := a.pointMap.Load(hash)
	if !ok {
		return ErrNotStored
	}

	stored := item.(*accumulatedData)
	atomic.AddUint64(&stored.count, 1)

	return nil
}

// Store - stores a reference
func (a *Accumulator) Store(item interface{}, ttl time.Duration) (string, error) {

	instance, err := a.transport.DataChannelItemToAccumulatedData(a.configuration, item, true)
	if err != nil {
		return empty, err
	}

	hash := instance.GetHash()

	err = a.store(hash, instance, ttl)
	if err != nil {
		return empty, err
	}

	return hash, nil
}

// StoreCustomHash - stores a custom reference
func (a *Accumulator) StoreCustomHash(item interface{}, ttl time.Duration, hash string) error {

	instance, err := a.transport.DataChannelItemToAccumulatedData(a.configuration, item, false)
	if err != nil {
		return err
	}

	err = a.store(hash, instance, ttl)
	if err != nil {
		return err
	}

	return nil
}

// store - shared store function
func (a *Accumulator) store(hash string, instance Hashable, ttl time.Duration) error {

	if _, loaded := a.pointMap.LoadOrStore(hash, instance); loaded {
		if logh.WarnEnabled {
			a.loggers.Warn().Msgf("a key was replaced on storage operation: %s", hash)
		}

		if a.ttlManager.Exists(hash) {
			a.ttlManager.RemoveTask(hash)
		}
	}

	data := instance.(*accumulatedData)
	data.lastUpdate = time.Now()
	data.logger = a.loggers
	data.pointMap = &a.pointMap
	data.ttl = ttl
	data.ttlManager = a.ttlManager
	data.hash = hash

	if ttl > 0 {
		err := a.ttlManager.AddTask(scheduler.NewTask(hash, ttl, data), true)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetName - returns the processor's name
func (a *Accumulator) GetName() string {
	return AccumulatorName
}

// Stop - terminates the processing cycle
func (a *Accumulator) Stop() {
	a.dataProcessorCore.Stop()
	a.ttlManager.RemoveAllTasks()
}
