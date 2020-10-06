package timeline

import (
	"fmt"
	"sync"

	"github.com/uol/logh"
)

/**
* The timeline's point flattener to reduce the number of points from a short time range.
* @author rnojiri
**/

// FlatOperation - the type of the aggregation used
type FlatOperation uint8

const (
	// FlattenerName - the name
	FlattenerName string = "flattener"

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
	hash  string
	value float64
}

// GetHash - returns the hash
func (fp *FlattenerPoint) GetHash() string {
	return fp.hash
}

// Flattener - controls the timeline's point flattening
type Flattener struct {
	dataProcessorCore
}

// mapEntry - a map entry containing all values from a point
type mapEntry struct {
	flattenerPointData
	values chan float64
}

// Clone - does a struct copy
func (me *mapEntry) Clone() interface{} {

	return &mapEntry{
		flattenerPointData: flattenerPointData{
			operation:       me.operation,
			timestamp:       me.timestamp,
			dataChannelItem: me.dataChannelItem,
		},
		values: me.values,
	}
}

// ReleaseResources - release this item resources
func (me *mapEntry) ReleaseResources() {

	close(me.values)
}

// NewFlattener - creates a new flattener
func NewFlattener(configuration *DataTransformerConfig) *Flattener {

	configuration.isSHAKE = isShakeAlgorithm(configuration.HashingAlgorithm)

	f := &Flattener{
		dataProcessorCore: dataProcessorCore{
			configuration: configuration,
			pointMap:      sync.Map{},
		},
	}

	f.parent = f

	return f
}

// BuildContextualLogger - build the contextual logger using more info
func (f *Flattener) BuildContextualLogger(path ...string) {

	logContext := []string{"pkg", "timeline/flattener"}

	if len(path) > 0 {
		logContext = append(logContext, path...)
	}

	f.loggers = logh.CreateContextualLogger(logContext...)
}

// Add - adds a new entry to the flattening process
func (f *Flattener) Add(point *FlattenerPoint) error {

	item, ok := f.pointMap.Load(point.hash)
	if ok {
		entry := item.(*mapEntry)
		entry.values <- point.value
		return nil
	}

	entry := &mapEntry{
		values: make(chan float64, f.configuration.PointValueBufferSize),
		flattenerPointData: flattenerPointData{
			operation:       point.operation,
			timestamp:       point.timestamp,
			dataChannelItem: point.dataChannelItem,
		},
	}

	entry.values <- point.value

	f.pointMap.Store(point.hash, entry)

	return nil
}

// ProcessMapEntry - process the values from an entry
func (f *Flattener) ProcessMapEntry(entry dataProcessorEntry) bool {

	copy := entry.Clone()

	newValue, err := f.flatten(copy.(*mapEntry))
	if err != nil {
		if logh.ErrorEnabled {
			ev := f.loggers.Error()
			if f.dataProcessorCore.configuration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(err).Msg("error on flatten operation")
		}

		return false
	}

	item, err := f.transport.FlattenerPointToDataChannelItem(newValue)
	if err != nil {
		if logh.ErrorEnabled {
			ev := f.loggers.Error()
			if f.dataProcessorCore.configuration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(err).Msg("error on casting operation")
		}

		return false
	}

	f.transport.DataChannel(item)

	return true
}

// flatten - flats the values using the specified operation
func (f *Flattener) flatten(entry *mapEntry) (*FlattenerPoint, error) {

	var flatValue float64
	var size float64

	switch entry.operation {

	case Avg, Sum, Count:

	fAvgSumCount:
		for {
			select {
			case v := <-(entry.values):
				flatValue += v
				size++
			default:
				break fAvgSumCount
			}
		}

	case Min:

		select {
		case v := <-(entry.values):
			flatValue = v
		default:
		}

	fMin:
		for {
			select {
			case v := <-(entry.values):
				if v < flatValue {
					flatValue = v
				}
			default:
				break fMin
			}
		}

	case Max:

		select {
		case v := <-(entry.values):
			flatValue = v
		default:
		}

	fMax:
		for {
			select {
			case v := <-(entry.values):
				if v > flatValue {
					flatValue = v
				}
			default:
				break fMax
			}
		}

	default:

		return nil, fmt.Errorf("operation id %d is not mapped", entry.operation)
	}

	switch entry.operation {
	case Avg:
		flatValue /= size
	case Count:
		flatValue = size
	}

	return &FlattenerPoint{
		flattenerPointData: entry.flattenerPointData,
		value:              flatValue,
	}, nil
}

// GetName - returns the processor's name
func (f *Flattener) GetName() string {
	return FlattenerName
}
