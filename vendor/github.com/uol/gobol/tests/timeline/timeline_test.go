package timeline_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"

	"github.com/uol/gobol/tester/httpserver"
	"github.com/uol/gobol/timeline"
)

var defaultTags = map[string]string{
	"host": "unit-test-host",
	"ttl":  "1",
}

// createTimeseriesBackend - creates a new test server simulating a timeseries backend
func createTimeseriesBackend() *httpserver.HTTPServer {

	headers := http.Header{}
	headers.Add("Content-type", "application/json")

	responses := httpserver.ResponseData{
		RequestData: httpserver.RequestData{
			URI:     "/api/put",
			Method:  "PUT",
			Headers: headers,
		},
		Status: 201,
	}

	return httpserver.CreateNewTestHTTPServer([]httpserver.ResponseData{responses})
}

// createTimelineManager - creates a new timeline manager
func createTimelineManager() *timeline.Manager {

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	transportConf := timeline.HTTPTransportConfig{
		ServiceEndpoint:        "/api/put",
		RequestTimeout:         "1s",
		BatchSendInterval:      "1s",
		BufferSize:             5,
		Method:                 "PUT",
		ExpectedResponseStatus: 201,
	}

	transport, err := timeline.NewHTTPTransport(&transportConf, logger)
	if err != nil {
		panic(err)
	}

	backend := timeline.Backend{
		Host: httpserver.TestServerHost,
		Port: httpserver.TestServerPort,
	}

	manager, err := timeline.NewManager(transport, &backend, defaultTags)
	if err != nil {
		panic(err)
	}

	return manager
}

// newNumberPoint - creates a new number point
func newNumberPoint(value float64) *timeline.NumberPoint {

	return &timeline.NumberPoint{
		Point: timeline.Point{
			Metric:    "number-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "number",
				"customTag": "number-test",
			},
		},
		Value: value,
	}
}

// newTextPoint - creates a new text point
func newTextPoint(text string) *timeline.TextPoint {

	return &timeline.TextPoint{
		Point: timeline.Point{
			Metric:    "text-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "text",
				"customTag": "text-test",
			},
		},
		Text: text,
	}
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

			var actual []timeline.NumberPoint
			fmt.Println(requestData.Body)
			err := json.Unmarshal([]byte(requestData.Body), &actual)
			if !assert.Nil(t, err, "error unmarshalling to number point") {
				return false
			}

			return testNumberPoint(t, expected, actual)
		}

		var actual []timeline.TextPoint
		err := json.Unmarshal([]byte(requestData.Body), &actual)
		if !assert.Nil(t, err, "error unmarshalling to text point") {
			return false
		}

		return testTextPoint(t, expected, actual)
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

	expectedNumbers, ok := expected.([]*timeline.TextPoint)
	if !ok && !assert.True(t, ok, "expected value must be a text point type") {
		return false
	}

	actualNumbers, ok := actual.([]timeline.TextPoint)
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

	expectedNumbers, ok := expected.([]*timeline.NumberPoint)
	if !ok && !assert.True(t, ok, "expected value must be a number point type") {
		return false
	}

	actualNumbers, ok := actual.([]timeline.NumberPoint)
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

// TestSendNumber - tests when the lib fires a event
func TestSendNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager()
	defer m.Shutdown()

	number := newNumberPoint(1)

	err := m.SendNumberPoint(number)
	assert.NoError(t, err, "no error expected when sending number")

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*timeline.NumberPoint{number}, true)
}

// TestSendText - tests when the lib fires a event
func TestSendText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager()
	defer m.Shutdown()

	text := newTextPoint("test")

	err := m.SendTextPoint(text)
	assert.NoError(t, err, "no error expected when sending text")

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*timeline.TextPoint{text}, false)
}

// TestSendNumberArray - tests when the lib fires a event
func TestSendNumberArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager()
	defer m.Shutdown()

	numbers := []*timeline.NumberPoint{newNumberPoint(1), newNumberPoint(2), newNumberPoint(3)}

	for _, n := range numbers {
		err := m.SendNumberPoint(n)
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

	m := createTimelineManager()
	defer m.Shutdown()

	texts := []*timeline.TextPoint{newTextPoint("1"), newTextPoint("2"), newTextPoint("3")}

	for _, n := range texts {
		err := m.SendTextPoint(n)
		assert.NoError(t, err, "no error expected when sending text")
	}

	<-time.After(2 * time.Second)

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, texts, false)
}
