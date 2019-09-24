package timeline

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/uol/gobol/hashing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
* The timeline's point flattener to reduce the number of points from a short time range.
* @author rnojiri
**/

// FlatOperation - the type of the aggregation used
type FlatOperation uint8

const (
	// Avg - aggregation
	Avg FlatOperation = 0

	// Sum - aggregation
	Sum FlatOperation = 1

	// Count - aggregation
	Count FlatOperation = 2

	// Max - aggregation
	Max FlatOperation = 3

	// Min - aggregation
	Min FlatOperation = 4
)

// flattenerPointData - all common properties from a point
type flattenerPointData struct {
	operation       FlatOperation
	timestamp       int64
	dataChannelItem interface{}
}

// FlattenerPoint - a flattener's point containing the value
type FlattenerPoint struct {
	flattenerPointData
	hashParameters []interface{}
	value          float64
}

// FlattenerConfig - flattener configuration
type FlattenerConfig struct {
	CycleDuration    time.Duration
	HashingAlgorithm hashing.Algorithm
}

// Flattener - controls the timeline's point flattening
type Flattener struct {
	configuration *FlattenerConfig
	pointMap      sync.Map
	terminateChan chan struct{}
	transport     Transport
	logger        *zap.Logger
}

// mapEntry - a map entry containing all values from a point
type mapEntry struct {
	flattenerPointData
	values []float64
}

// NewFlattener - creates a new flattener
func NewFlattener(transport Transport, configuration *FlattenerConfig, logger *zap.Logger) (*Flattener, error) {

	if transport == nil {
		return nil, fmt.Errorf("transport implementation is required")
	}

	f := &Flattener{
		configuration: configuration,
		pointMap:      sync.Map{},
		terminateChan: make(chan struct{}, 1),
		transport:     transport,
		logger:        logger,
	}

	return f, nil
}

// Start - starts the flattenner and the transport
func (f *Flattener) Start() error {

	go f.beginCycle()

	return f.transport.Start()
}

// beginCycle - begins the flattening loop cycle
func (f *Flattener) beginCycle() {

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "Flattener"),
		zap.String("func", "beginCycle"),
	}

	f.logger.Info("starting flattening cycle")

	for {
		<-time.After(f.configuration.CycleDuration)

		select {
		case <-f.terminateChan:
			f.logger.Info("breaking flattening cycle", lf...)
			return
		default:
		}

		count := 0

		f.pointMap.Range(func(k, v interface{}) bool {

			entry := v.(*mapEntry)

			f.processEntry(entry)

			f.pointMap.Delete(k)

			count++

			return true
		})

		f.logger.Info(fmt.Sprintf("%d points were flattened", count), lf...)
	}
}

// Add - adds a new entry to the flattening process
func (f *Flattener) Add(point *FlattenerPoint) error {

	hash, err := hashing.Generate(f.configuration.HashingAlgorithm, point.hashParameters...)
	if err != nil {
		return err
	}

	key := hex.EncodeToString(hash)

	item, ok := f.pointMap.Load(key)
	if ok {
		entry := item.(*mapEntry)
		entry.values = append(entry.values, point.value)
		return nil
	}

	entry := &mapEntry{
		values: []float64{point.value},
		flattenerPointData: flattenerPointData{
			operation:       point.operation,
			timestamp:       point.timestamp,
			dataChannelItem: point.dataChannelItem,
		},
	}

	f.pointMap.Store(key, entry)

	return nil
}

// processEntry - process the values from an entry
func (f *Flattener) processEntry(entry *mapEntry) {

	newValue, err := f.flatten(entry)
	if err != nil {

		lf := []zapcore.Field{
			zap.String("package", "timeline"),
			zap.String("struct", "Flattener"),
			zap.String("func", "processEntry"),
		}

		f.logger.Error(err.Error(), lf...)
		return
	}

	item, err := f.transport.FlattenedPointToDataChannelItem(newValue)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "timeline"),
			zap.String("struct", "Flattener"),
			zap.String("func", "processEntry"),
		}

		f.logger.Error(err.Error(), lf...)
		return
	}

	f.transport.DataChannel() <- item
}

// flatten - flats the values using the specified operation
func (f *Flattener) flatten(entry *mapEntry) (*FlattenerPoint, error) {

	var flatValue float64

	switch entry.operation {

	case Avg:

		for _, v := range entry.values {
			flatValue += v
		}

		flatValue /= (float64)(len(entry.values))

	case Sum:

		for _, v := range entry.values {
			flatValue += v
		}

	case Count:

		flatValue = (float64)(len(entry.values))

	case Min:

		flatValue = entry.values[0]

		for i := 1; i < len(entry.values); i++ {

			if entry.values[i] < flatValue {
				flatValue = entry.values[i]
			}
		}

	case Max:

		flatValue = entry.values[0]

		for i := 1; i < len(entry.values); i++ {

			if entry.values[i] > flatValue {
				flatValue = entry.values[i]
			}
		}

	default:

		return nil, fmt.Errorf("operation id %d is not mapped", entry.operation)
	}

	return &FlattenerPoint{
		flattenerPointData: entry.flattenerPointData,
		value:              flatValue,
	}, nil
}

// Close - terminates the flattener and the transport
func (f *Flattener) Close() {

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "Flattener"),
		zap.String("func", "Close"),
	}

	f.logger.Info("closing...", lf...)

	f.transport.Close()

	f.terminateChan <- struct{}{}
}
