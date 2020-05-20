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

	jsonEscapedDoubleQuote   string = "\\\""
	jsonEscapedEscapeBar     string = "\\\\"
	strBracketLeft           string = "{"
	strBracketRight          string = "}"
	strSquareBracketLeft     string = "["
	strSquareBracketRight    string = "]"
	strComma                 string = ","
	strDoubleQuote           string = `"`
	strColon                 string = ":"
	strDot                   string = "."
	strJSON                  string = "json"
	strFmtStringInBrackets   string = "{%s}"
	strFmtStringInSqBrackets string = "[%s]"
	strStringVar             string = "%s"
	strFloatVar              string = "%f"
	strIntVar                string = "%d"
	strBooleanVar            string = "%t"
)

var (
	byteValueDoubleQuote = ([]byte(strDoubleQuote))[0]
	byteValueEscapeBar   = ([]byte("\\"))[0]
)

// mappedJSON - internal mapped JSON struct
type mappedJSON struct {
	format       string
	formatSize   int
	variableMap  map[string]int
	numVariables int
}

// Point - the base point
type Point struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
}

// NumberPoint - a point with number type value
type NumberPoint struct {
	Point
	Value float64 `json:"value"`
}

// TextPoint - a point with text type value
type TextPoint struct {
	Point
	Text string `json:"text"`
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
