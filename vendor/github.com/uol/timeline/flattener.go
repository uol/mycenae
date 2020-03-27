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
	values []float64
}

// NewFlattener - creates a new flattener
func NewFlattener(configuration *DataTransformerConf) *Flattener {

	configuration.isSHAKE = isShakeAlgorithm(configuration.HashingAlgorithm)

	f := &Flattener{
		dataProcessorCore: dataProcessorCore{
			configuration: configuration,
			pointMap:      sync.Map{},
			terminateChan: make(chan struct{}, 1),
			loggers:       logh.CreateContextualLogger("pkg", "timeline/flattener"),
		},
	}

	f.parent = f

	return f
}

// Add - adds a new entry to the flattening process
func (f *Flattener) Add(point *FlattenerPoint) error {

	item, ok := f.pointMap.Load(point.hash)
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

	f.pointMap.Store(point.hash, entry)

	return nil
}

// ProcessMapEntry - process the values from an entry
func (f *Flattener) ProcessMapEntry(entry interface{}) bool {

	newValue, err := f.flatten(entry.(*mapEntry))
	if err != nil {
		if logh.ErrorEnabled {
			f.loggers.Error().Msg(err.Error())
		}

		return false
	}

	item, err := f.transport.FlattenerPointToDataChannelItem(newValue)
	if err != nil {
		if logh.ErrorEnabled {
			f.loggers.Error().Msg(err.Error())
		}

		return false
	}

	f.transport.DataChannel() <- item

	return true
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

// GetName - returns the processor's name
func (f *Flattener) GetName() string {
	return FlattenerName
}
