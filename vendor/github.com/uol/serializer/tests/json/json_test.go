package json

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	serializer "github.com/uol/serializer/json"
	"github.com/uol/serializer/tests"
)

/**
* Has unit tests for the JSON serializer.
* @author rnojiri
**/

type SimpleJSON struct {
	Text    string  `json:"text"`
	Integer int     `json:"integer"`
	Float   float64 `json:"float"`
	Boolean bool    `json:"boolean"`
}

type ComplexTypeJSON struct {
	Simple SimpleJSON `json:"simple"`
	CollectionJSON
}

type CollectionJSON struct {
	Mapping map[string]int `json:"mapping"`
	Array   []float64      `json:"array"`
}

// createSerializer - creates a new serializer
func createSerializer() *serializer.Serializer {

	return serializer.New(100)
}

// addType - add a new serialized type
func addType(t *testing.T, s *serializer.Serializer, name string, newType interface{}, vars ...string) {

	err := s.Add(name, newType, vars...)
	if !assert.NoError(t, err, "error adding a new serialization type") {
		panic(err)
	}
}

// serialize - try to serialize the named type
func serialize(t *testing.T, s *serializer.Serializer, name string, params ...interface{}) string {

	result, err := s.Serialize(name, params...)
	if !assert.NoError(t, err, "error serializing the type: %s", name) {
		panic(err)
	}

	return result
}

// validateJSON - validates the JSON
func validateJSON(t *testing.T, strJSON string, expected interface{}, actualType interface{}) bool {

	err := json.Unmarshal([]byte(strJSON), actualType)
	if !assert.NoError(t, err, "error unmarshalling json: %s", strJSON) {
		return false
	}

	return assert.True(t, reflect.DeepEqual(expected, actualType), "expected equal objects %+v != %+v", expected, actualType)
}

// TestNoVariables - test serializing a simple JSON with no complex types and no variables
func TestNoVariables(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "test",
	}

	s := createSerializer()
	addType(t, s, "s", newType)

	result := serialize(t, s, "s")

	actual := SimpleJSON{}
	validateJSON(t, result, &newType, &actual)
}

// TestArrayNoVariables - test serializing an array of simple JSONs with no complex types
func TestArrayNoVariables(t *testing.T) {

	newType := SimpleJSON{
		Boolean: false,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "array",
	}

	s := createSerializer()
	addType(t, s, "s", newType)

	result, err := s.SerializeArray([]*serializer.ArrayItem{
		{Name: "s"},
		{Name: "s"},
		{Name: "s"},
	}...)
	if !assert.NoError(t, err, "error serializing to array") {
		return
	}

	array := []SimpleJSON{
		newType,
		newType,
		newType,
	}

	actual := []SimpleJSON{}
	validateJSON(t, result, &array, &actual)
}

// TestVariables - test serializing a simple JSON with complex types
func TestVariables(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "variable",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "boolean", "text")

	result := serialize(t, s, "s",
		"boolean", false,
		"text", "changed",
	)

	expected := SimpleJSON{
		Boolean: false,
		Float:   newType.Float,
		Integer: newType.Integer,
		Text:    "changed",
	}

	actual := SimpleJSON{}
	validateJSON(t, result, &expected, &actual)
}

// TestArrayVariables - test serializing an array of simple JSONs with no complex types
func TestArrayVariables(t *testing.T) {

	newType := SimpleJSON{
		Boolean: false,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "array",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "boolean", "float", "integer")

	result, err := s.SerializeArray([]*serializer.ArrayItem{
		{Name: "s", Parameters: []interface{}{"boolean", true, "float", 1.0, "integer", 1}},
		{Name: "s", Parameters: []interface{}{"boolean", false, "float", 2.0, "integer", 2}},
		{Name: "s", Parameters: []interface{}{"boolean", true, "float", 3.0, "integer", 3}},
	}...)
	if !assert.NoError(t, err, "error serializing to array") {
		return
	}

	array := []SimpleJSON{
		{
			Boolean: true,
			Float:   1.0,
			Integer: 1,
			Text:    "array",
		},
		{
			Boolean: false,
			Float:   2.0,
			Integer: 2,
			Text:    "array",
		},
		{
			Boolean: true,
			Float:   3.0,
			Integer: 3,
			Text:    "array",
		},
	}

	actual := []SimpleJSON{}
	validateJSON(t, result, &array, &actual)
}

// TestCompositeStructJSON - test a complex json serialization
func TestCompositeStructJSON(t *testing.T) {

	p := serializer.NumberPoint{
		Point: serializer.Point{
			Metric:    "metric1",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "here",
				"ttl":  "1",
			},
		},
		Value: 1.0,
	}

	s := createSerializer()
	addType(t, s, "c", p, "value", "tags.host")
	result := serialize(t, s, "c",
		"value", 100.5,
		"tags.host", "loghost",
	)

	expected := serializer.NumberPoint{
		Point: serializer.Point{
			Metric:    p.Metric,
			Timestamp: p.Timestamp,
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "loghost",
				"ttl":  "1",
			},
		},
		Value: 100.5,
	}

	actual := serializer.NumberPoint{}
	validateJSON(t, result, &expected, &actual)
}

// TestComplexTypeJSON - test serializing a simple JSON with complex types
func TestComplexTypeJSON(t *testing.T) {

	newType := ComplexTypeJSON{
		Simple: SimpleJSON{
			Boolean: true,
			Float:   99,
			Integer: -9,
			Text:    "complex",
		},
		CollectionJSON: CollectionJSON{
			Mapping: map[string]int{
				"1": 1,
				"2": 2,
				"3": 3,
			},
			Array: []float64{1.0, 2.0, 3.0},
		},
	}

	s := createSerializer()
	addType(t, s, "s", newType, "simple.text", "mapping.1", "array[1]")

	result := serialize(t, s, "s",
		"simple.text", "changed",
		"mapping.1", -1,
		"array[1]", -2.0,
	)

	expected := ComplexTypeJSON{
		Simple: SimpleJSON{
			Boolean: true,
			Float:   99,
			Integer: -9,
			Text:    "changed",
		},
		CollectionJSON: CollectionJSON{
			Mapping: map[string]int{
				"1": -1,
				"2": 2,
				"3": 3,
			},
			Array: []float64{1.0, -2.0, 3.0},
		},
	}

	actual := ComplexTypeJSON{}
	validateJSON(t, result, &expected, &actual)
}

// TestVariableCollections - test serializing a simple JSON with complex types
func TestVariableCollections(t *testing.T) {

	newType := CollectionJSON{

		Mapping: map[string]int{
			"1": 1,
			"2": 2,
			"3": 3,
		},

		Array: []float64{1.0, 2.0, 3.0},
	}

	s := createSerializer()
	addType(t, s, "s", newType, "mapping", "array")

	result := serialize(t, s, "s",

		"mapping", map[string]interface{}{
			"4": 4,
			"5": 5,
			"6": 6,
		},

		"array", []float64{4.0, 5.0, 6.0},
	)

	expected := CollectionJSON{

		Mapping: map[string]int{
			"4": 4,
			"5": 5,
			"6": 6,
		},

		Array: []float64{4.0, 5.0, 6.0},
	}

	actual := CollectionJSON{}
	validateJSON(t, result, &expected, &actual)
}

// TestJSONEncoding - test serializing a JSON with double quotes as values
func TestJSONEncoding(t *testing.T) {

	newType := SimpleJSON{
		Boolean: false,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    `"unchanged"`,
	}

	s := createSerializer()
	addType(t, s, "text-const", newType)
	addType(t, s, "text-variable", newType, "text")

	result := serialize(t, s, "text-const")
	expected := newType

	actual := SimpleJSON{}
	validateJSON(t, result, &expected, &actual)

	result = serialize(t, s, "text-variable", "text", `"changed"`)
	expected = SimpleJSON{
		Boolean: false,
		Float:   newType.Float,
		Integer: newType.Integer,
		Text:    `"changed"`,
	}

	validateJSON(t, result, &expected, &actual)
}

// TestGenericSerializer - test using the generic way to serialize
func TestGenericSerializer(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "generic",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "boolean", "text")

	result1 := serialize(t, s, "s",
		"boolean", false,
		"text", "changed",
	)

	result2, err := s.SerializeGeneric(&serializer.ArrayItem{
		Name: "s",
		Parameters: []interface{}{
			"boolean", false,
			"text", "changed",
		},
	})

	if !assert.NoError(t, err, "error using generic serialization") {
		panic(err)
	}

	assert.Equal(t, result1, result2, "expected same output")
}

// TestGenericArraySerializer - test using the generic way to serialize
func TestGenericArraySerializer(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "generic",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "boolean", "float", "integer")

	itemArray := []*serializer.ArrayItem{
		{Name: "s", Parameters: []interface{}{"boolean", true, "float", 1.0, "integer", 1}},
		{Name: "s", Parameters: []interface{}{"boolean", false, "float", 2.0, "integer", 2}},
		{Name: "s", Parameters: []interface{}{"boolean", true, "float", 3.0, "integer", 3}},
	}

	result1, err := s.SerializeArray(itemArray...)
	if !assert.NoError(t, err, "error serializing to array") {
		return
	}

	interfaceArray := []interface{}{
		itemArray[0], itemArray[1], itemArray[2],
	}

	result2, err := s.SerializeGenericArray(interfaceArray...)

	if !assert.NoError(t, err, "error using generic array serialization") {
		panic(err)
	}

	assert.Equal(t, result1, result2, "expected same output")
}

// TestInvalidNumberOfTags - tests a single line, invalid number of string tags
func TestInvalidNumberOfTags(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "variable",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "boolean", "text")

	_, err := s.Serialize("s", "boolean")
	assert.Error(t, err, "expected validation error")
}

// TestArrayWithInvalidNumberOfTags - tests an array of items, invalid number of string tags
func TestArrayWithInvalidNumberOfTags(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "variable",
	}

	s := createSerializer()
	addType(t, s, "s", newType, "integer", "float")

	items := []*serializer.ArrayItem{
		{Name: "s", Parameters: []interface{}{"integer", 1, "float", 10.0}},
		{Name: "s", Parameters: []interface{}{"integer", 1, "float", 10.0}},
		{Name: "s", Parameters: []interface{}{"integer", 1, 10.0}},
	}

	_, err := s.SerializeArray(items...)
	assert.Error(t, err, "expected validation error")
}

// TestSpecialChars - test serializing a simple JSON with special characters
func TestSpecialChars(t *testing.T) {

	newType := SimpleJSON{
		Boolean: true,
		Float:   float64(tests.GenerateRandom(0, 100)),
		Integer: tests.GenerateRandom(0, 1000),
		Text:    "\\\"test\"",
	}

	s := createSerializer()
	addType(t, s, "s", newType)

	result := serialize(t, s, "s")

	actual := SimpleJSON{}
	validateJSON(t, result, &newType, &actual)
}
