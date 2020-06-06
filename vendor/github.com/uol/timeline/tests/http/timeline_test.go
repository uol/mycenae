package timeline_http_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	gotesthttp "github.com/uol/gotest/http"
	serializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManager - creates a new timeline manager
func createTimelineManager(start bool, transportSize int, batchSendInterval time.Duration) *timeline.Manager {

	backend := timeline.Backend{
		Host: testServerHost,
		Port: testServerPort,
	}

	transport := createHTTPTransport(transportSize, batchSendInterval)

	manager, err := timeline.NewManager(transport, nil, nil, &backend)
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
func testSerializeCompareNumber(t *testing.T, text string, expected interface{}, ignoreTimestamp bool) bool {

	var actual []serializer.NumberPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to number point") {
		return false
	}

	return testNumberPoint(t, expected, actual, ignoreTimestamp)
}

// testSerializeCompareText - compares a serialized json and a json struct
func testSerializeCompareText(t *testing.T, text string, expected interface{}) bool {

	var actual []serializer.TextPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to text point") {
		return false
	}

	return testTextPoint(t, expected, actual)
}

// testRequestData - tests the request data
func testRequestData(t *testing.T, requestData *gotesthttp.RequestData, expected interface{}, isNumber, ignoreTimestamp bool) bool {

	result := true

	result = result && assert.NotNil(t, requestData, "request data cannot be null")
	result = result && assert.Equal(t, "/api/put", requestData.URI, "expected /api/put as endpoint")
	result = result && assert.Equal(t, "PUT", requestData.Method, "expected PUT as method")
	result = result && assert.Equal(t, "application/json", requestData.Headers.Get("Content-type"), "expected aplication/json as content-type header")

	if result {

		if isNumber {
			return testSerializeCompareNumber(t, requestData.Body, expected, ignoreTimestamp)
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

	expectedNumbers, ok := expected.([]*serializer.TextPoint)
	if !ok && !assert.True(t, ok, "expected value must be a text point type") {
		return false
	}

	actualNumbers, ok := actual.([]serializer.TextPoint)
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
func testNumberPoint(t *testing.T, expected interface{}, actual interface{}, ignoreTimestamp bool) bool {

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	expectedNumbers, ok := expected.([]*serializer.NumberPoint)
	if !ok && !assert.True(t, ok, "expected value must be a number point type") {
		return false
	}

	actualNumbers, ok := actual.([]serializer.NumberPoint)
	if !ok && !assert.True(t, ok, "actual value must be a number point type") {
		return false
	}

	sort.Sort(ByMetricP(expectedNumbers))
	sort.Sort(ByMetric(actualNumbers))

	if !assert.Len(t, actualNumbers, len(expectedNumbers), "expected %d number points", len(expectedNumbers)) {
		return false
	}

	result := true

	for i := 0; i < len(expectedNumbers); i++ {

		result = result && assert.Equal(t, expectedNumbers[i].Metric, actualNumbers[i].Metric, "number point's metric differs")

		if !ignoreTimestamp {
			result = result && assert.Equal(t, expectedNumbers[i].Timestamp, actualNumbers[i].Timestamp, "number point's timestamp differs")
		} else {
			result = result && assert.Greater(t, expectedNumbers[i].Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
			result = result && assert.Greater(t, actualNumbers[i].Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
		}

		result = result && assert.True(t, reflect.DeepEqual(expectedNumbers[i].Tags, actualNumbers[i].Tags), "number point's tags differs")
		result = result && assert.Equal(t, expectedNumbers[i].Value, actualNumbers[i].Value, "number point's value differs")

		if !result {
			return false
		}
	}

	return result
}

// toGenericParametersN - converts a number point to generic parameters
func toGenericParametersN(point *serializer.NumberPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"timestamp", point.Timestamp,
		"value", point.Value,
		"tags", point.Tags,
	}
}

// toGenericParametersT - converts a number point to generic parameters
func toGenericParametersT(point *serializer.TextPoint) []interface{} {

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

	m := createTimelineManager(true, defaultTransportSize, time.Second)
	defer m.Shutdown()

	number := newNumberPoint(1)

	err := m.SendHTTP(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*serializer.NumberPoint{number}, true, false)
}

// TestSendText - tests when the lib fires a event
func TestSendText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second)
	defer m.Shutdown()

	text := newTextPoint("test")

	err := m.SendHTTP(textPoint, toGenericParametersT(text)...)
	assert.NoError(t, err, "no error expected when sending text")

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*serializer.TextPoint{text}, false, false)
}

// TestSendNumberArray - tests when the lib fires a event
func TestSendNumberArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second)
	defer m.Shutdown()

	numbers := []*serializer.NumberPoint{newNumberPoint(1), newNumberPoint(2), newNumberPoint(3)}

	for _, n := range numbers {
		err := m.SendHTTP(numberPoint, toGenericParametersN(n)...)
		assert.NoError(t, err, "no error expected when sending number")
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, numbers, true, false)
}

// TestSendTextArray - tests when the lib fires a event
func TestSendTextArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second)
	defer m.Shutdown()

	texts := []*serializer.TextPoint{newTextPoint("1"), newTextPoint("2"), newTextPoint("3")}

	for _, n := range texts {
		err := m.SendHTTP(textPoint, toGenericParametersT(n)...)
		assert.NoError(t, err, "no error expected when sending text")
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, texts, false, false)
}

// TestSendCustomNumber - tests configuring the json variables
func TestSendCustomNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(false, defaultTransportSize, time.Second)
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

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*serializer.NumberPoint{number}, true, false)
}

// TestSendCustomText - tests configuring the json variables
func TestSendCustomText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(false, defaultTransportSize, time.Second)
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

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*serializer.TextPoint{text}, false, false)
}

// TestNumberSerialization - tests configuring the json variables
func TestNumberSerialization(t *testing.T) {

	m := createTimelineManager(false, defaultTransportSize, time.Second)
	defer m.Shutdown()

	number := newNumberPoint(15)

	serialized, err := m.SerializeHTTP(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when serializing number") {
		return
	}

	testSerializeCompareNumber(t, fmt.Sprintf("[%s]", serialized), []*serializer.NumberPoint{number}, false)
}

// TestTextSerialization - tests configuring the json variables
func TestTextSerialization(t *testing.T) {

	m := createTimelineManager(false, defaultTransportSize, time.Second)
	defer m.Shutdown()

	text := newTextPoint("serialization")

	serialized, err := m.SerializeHTTP(textPoint, toGenericParametersT(text)...)
	if !assert.NoError(t, err, "no error expected when serializing text") {
		return
	}

	testSerializeCompareText(t, fmt.Sprintf("[%s]", serialized), []*serializer.TextPoint{text})
}

// TestExceedingBufferSize - tests when the buffer exceeds its size
func TestExceedingBufferSize(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	bufferSize := 2
	numPoints := 6
	numRequests := numPoints / bufferSize
	requests := make([]*gotesthttp.RequestData, numRequests)
	batchSendInterval := 5 * time.Second
	wc := sync.WaitGroup{}
	wc.Add(numRequests)

	go func() {
		for i := 0; i < numRequests; i++ {
			requests[i] = <-s.RequestChannel()
			wc.Done()
		}
	}()

	m := createTimelineManager(true, bufferSize, batchSendInterval)
	defer m.Shutdown()

	numbers := make([]*serializer.NumberPoint, numPoints)
	firstPointTime := time.Now().Unix()

	for i := 0; i < numPoints; i++ {

		numbers[i] = newNumberPoint(float64(i))

		err := m.SendHTTP(numberPoint, toGenericParametersN(numbers[i])...)
		if !assert.NoError(t, err, "no error expected when sending number") {
			return
		}
	}

	wc.Wait()

	// the time between requests must be minimal and the last request will be $batchSendDuration
	for i := 0; i < numRequests-1; i++ {
		if !assert.LessOrEqual(t, requests[i].Date.Unix()-firstPointTime, int64(10), "expected less than 10 milisecond") {
			return
		}
	}

	// the last time will be the same as $batchSendDuration
	assert.GreaterOrEqual(t, requests[numRequests-1].Date.Unix()-firstPointTime, int64(batchSendInterval.Seconds()), "expected greater than batch interval")
	assert.Less(t, requests[numRequests-1].Date.Unix()-firstPointTime, int64(batchSendInterval.Seconds()+1), "expected less than batch interval plus one second")
}
