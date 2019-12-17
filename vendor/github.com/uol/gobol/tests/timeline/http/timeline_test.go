package timeline_http_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uol/gobol/structs"
	"github.com/uol/gobol/tester/httpserver"
	"github.com/uol/gobol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManager - creates a new timeline manager
func createTimelineManager(start bool) *timeline.Manager {

	backend := timeline.Backend{
		Host: httpserver.TestServerHost,
		Port: httpserver.TestServerPort,
	}

	transport := createHTTPTransport()

	manager, err := timeline.NewManager(transport, &backend)
	if err != nil {
		panic(err)
	}

	if start {
		err = manager.Start()
		if err != nil {
			panic(err)
		}
	}

	return manager
}

// testSerializeCompareNumber - compares a serialized json and a json struct
func testSerializeCompareNumber(t *testing.T, text string, expected interface{}) bool {

	var actual []structs.NumberPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to number point") {
		return false
	}

	return testNumberPoint(t, expected, actual)
}

// testSerializeCompareText - compares a serialized json and a json struct
func testSerializeCompareText(t *testing.T, text string, expected interface{}) bool {

	var actual []structs.TextPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to text point") {
		return false
	}

	return testTextPoint(t, expected, actual)
}

// testRequestData - tests the request data
func testRequestData(t *testing.T, requestData *httpserver.RequestData, expected interface{}, isNumber bool) bool {

	result := true

	result = result && assert.NotNil(t, requestData, "request data cannot be null")
	result = result && assert.Equal(t, "/api/put", requestData.URI, "expected /api/put as endpoint")
	result = result && assert.Equal(t, "PUT", requestData.Method, "expected PUT as method")
	result = result && assert.Equal(t, "application/json", requestData.Headers.Get("Content-type"), "expected aplication/json as content-type header")

	if result {

		if isNumber {
			return testSerializeCompareNumber(t, requestData.Body, expected)
		}

		return testSerializeCompareText(t, requestData.Body, expected)
	}

	return result
}

// testTextPoint - compares two points
func testTextPoint(t *testing.T, expected interface{}, actual interface{}) bool {

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	expectedNumbers, ok := expected.([]*structs.TextPoint)
	if !ok && !assert.True(t, ok, "expected value must be a text point type") {
		return false
	}

	actualNumbers, ok := actual.([]structs.TextPoint)
	if !ok && !assert.True(t, ok, "actual value must be a text point type") {
		return false
	}

	if !assert.Len(t, actualNumbers, len(expectedNumbers), "expected %d text points", len(expectedNumbers)) {
		return false
	}

	result := true

	for i := 0; i < len(expectedNumbers); i++ {

		result = result && assert.Equal(t, expectedNumbers[i].Metric, actualNumbers[i].Metric, "text point's metric differs")
		result = result && assert.Equal(t, expectedNumbers[i].Timestamp, actualNumbers[i].Timestamp, "text point's timestamp differs")
		result = result && assert.True(t, reflect.DeepEqual(expectedNumbers[i].Tags, actualNumbers[i].Tags), "text point's tags differs")
		result = result && assert.Equal(t, expectedNumbers[i].Text, actualNumbers[i].Text, "text point's value differs")

		if !result {
			return false
		}
	}

	return result
}

// testNumberPoint - compares two points
func testNumberPoint(t *testing.T, expected interface{}, actual interface{}) bool {

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	expectedNumbers, ok := expected.([]*structs.NumberPoint)
	if !ok && !assert.True(t, ok, "expected value must be a number point type") {
		return false
	}

	actualNumbers, ok := actual.([]structs.NumberPoint)
	if !ok && !assert.True(t, ok, "actual value must be a number point type") {
		return false
	}

	if !assert.Len(t, actualNumbers, len(expectedNumbers), "expected %d number points", len(expectedNumbers)) {
		return false
	}

	result := true

	for i := 0; i < len(expectedNumbers); i++ {

		result = result && assert.Equal(t, expectedNumbers[i].Metric, actualNumbers[i].Metric, "number point's metric differs")
		result = result && assert.Equal(t, expectedNumbers[i].Timestamp, actualNumbers[i].Timestamp, "number point's timestamp differs")
		result = result && assert.True(t, reflect.DeepEqual(expectedNumbers[i].Tags, actualNumbers[i].Tags), "number point's tags differs")
		result = result && assert.Equal(t, expectedNumbers[i].Value, actualNumbers[i].Value, "number point's value differs")

		if !result {
			return false
		}
	}

	return result
}

// toGenericParametersN - converts a number point to generic parameters
func toGenericParametersN(point *structs.NumberPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"timestamp", point.Timestamp,
		"value", point.Value,
		"tags", point.Tags,
	}
}

// toGenericParametersT - converts a number point to generic parameters
func toGenericParametersT(point *structs.TextPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"timestamp", point.Timestamp,
		"text", point.Text,
		"tags", point.Tags,
	}
}

// TestSendNumber - tests when the lib fires a event
func TestSendNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true)
	defer m.Shutdown()

	number := newNumberPoint(1)

	err := m.SendHTTP(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*structs.NumberPoint{number}, true)
}

// TestSendText - tests when the lib fires a event
func TestSendText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true)
	defer m.Shutdown()

	text := newTextPoint("test")

	err := m.SendHTTP(textPoint, toGenericParametersT(text)...)
	assert.NoError(t, err, "no error expected when sending text")

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*structs.TextPoint{text}, false)
}

// TestSendNumberArray - tests when the lib fires a event
func TestSendNumberArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true)
	defer m.Shutdown()

	numbers := []*structs.NumberPoint{newNumberPoint(1), newNumberPoint(2), newNumberPoint(3)}

	for _, n := range numbers {
		err := m.SendHTTP(numberPoint, toGenericParametersN(n)...)
		assert.NoError(t, err, "no error expected when sending number")
	}

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, numbers, true)
}

// TestSendTextArray - tests when the lib fires a event
func TestSendTextArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true)
	defer m.Shutdown()

	texts := []*structs.TextPoint{newTextPoint("1"), newTextPoint("2"), newTextPoint("3")}

	for _, n := range texts {
		err := m.SendHTTP(textPoint, toGenericParametersT(n)...)
		assert.NoError(t, err, "no error expected when sending text")
	}

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, texts, false)
}

// TestSendCustomNumber - tests configuring the json variables
func TestSendCustomNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(false)
	defer m.Shutdown()

	number := newNumberPoint(1.0)

	transport := m.GetTransport().(*timeline.HTTPTransport)

	const custom = "customPoint"

	// only value is variable
	err := transport.AddJSONMapping(custom, *number, "value")
	if !assert.NoError(t, err, "no error adding custom configuration") {
		return
	}

	m.Start()

	err = m.SendHTTP(custom, "value", 5.0)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	number.Value = 5.0

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*structs.NumberPoint{number}, true)
}

// TestSendCustomText - tests configuring the json variables
func TestSendCustomText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(false)
	defer m.Shutdown()

	text := newTextPoint("woohoo")

	transport := m.GetTransport().(*timeline.HTTPTransport)

	const custom = "customPoint"

	// only value is variable
	err := transport.AddJSONMapping(custom, *text, "text")
	if !assert.NoError(t, err, "no error adding custom configuration") {
		return
	}

	m.Start()

	err = m.SendHTTP(custom, "text", "modified")
	if !assert.NoError(t, err, "no error expected when sending text") {
		return
	}

	text.Text = "modified"

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*structs.TextPoint{text}, false)
}

// TestNumberSerialization - tests configuring the json variables
func TestNumberSerialization(t *testing.T) {

	m := createTimelineManager(false)
	defer m.Shutdown()

	number := newNumberPoint(15)

	serialized, err := m.SerializeHTTP(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when serializing number") {
		return
	}

	testSerializeCompareNumber(t, fmt.Sprintf("[%s]", serialized), []*structs.NumberPoint{number})
}

// TestTextSerialization - tests configuring the json variables
func TestTextSerialization(t *testing.T) {

	m := createTimelineManager(false)
	defer m.Shutdown()

	text := newTextPoint("serialization")

	serialized, err := m.SerializeHTTP(textPoint, toGenericParametersT(text)...)
	if !assert.NoError(t, err, "no error expected when serializing text") {
		return
	}

	testSerializeCompareText(t, fmt.Sprintf("[%s]", serialized), []*structs.TextPoint{text})
}
