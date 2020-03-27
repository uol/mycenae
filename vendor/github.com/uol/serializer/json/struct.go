package json

import "github.com/uol/serializer/serializer"

/**
* Has all structs used by the JSON serializer.
* @author rnojiri
**/

type variableType uint8

const (
	normalValue      variableType = 0
	propertyVariable variableType = 1
)

var (
	doubleQuote        = ([]byte("\""))[0]
	escapedDoubleQuote = "\\\""
)

// mappedJSON - internal mapped JSON struct
type mappedJSON struct {
	format       string
	formatSize   int
	variableMap  map[string]int
	numVariables int
}

// Serializer - the json serializer
type Serializer struct {
	serializer.Serializer
	bufferSize int
	mapping    map[string]*mappedJSON
}

// ArrayItem - a configuration to render a json
type ArrayItem struct {
	Name       string
	Parameters []interface{}
}

// New - creates a new JSON serializer
func New(bufferSize int) *Serializer {

	return &Serializer{
		bufferSize: bufferSize,
		mapping:    map[string]*mappedJSON{},
	}
}
