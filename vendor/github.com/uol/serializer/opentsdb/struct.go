package opentsdb

import "github.com/uol/serializer/serializer"

/**
* Has all structs used by the OpenTSDB serializer.
* @author rnojiri
**/

// Serializer - the json serializer
type Serializer struct {
	serializer.Serializer
	bufferSize int
}

// ArrayItem - an array of parameters
type ArrayItem struct {
	Metric    string
	Timestamp int64
	Value     float64
	Tags      []interface{}
}

// New - creates a new JSON serializer
func New(bufferSize int) *Serializer {

	return &Serializer{
		bufferSize: bufferSize,
	}
}
