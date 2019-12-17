package timeline_http_test

import (
	"testing"
	"time"

	"github.com/uol/gobol/hashing"
	"github.com/uol/gobol/structs"
	"github.com/uol/gobol/tester/httpserver"
	"github.com/uol/gobol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerF - creates a new timeline manager
func createTimelineManagerF(start bool) *timeline.Manager {

	backend := timeline.Backend{
		Host: httpserver.TestServerHost,
		Port: httpserver.TestServerPort,
	}

	transport := createHTTPTransport()

	conf := &timeline.FlattenerConfig{
		CycleDuration:    time.Millisecond * 900,
		HashingAlgorithm: hashing.SHA256,
	}

	flattener, err := timeline.NewFlattener(transport, conf)
	if err != nil {
		panic(err)
	}

	manager, err := timeline.NewManagerF(flattener, &backend)
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

// toGenericParameters - converts a number point to generic parameters
func toGenericParameters(point *structs.NumberPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"tags", point.Tags,
		"value", point.Value,
		"timestamp", point.Timestamp,
	}
}

// testFlatOperation - tests some operation
func testFlatOperation(t *testing.T, operation timeline.FlatOperation, expectedValue float64, opValues ...float64) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerF(true)
	defer m.Shutdown()

	number := newNumberPoint(expectedValue)

	for _, v := range opValues {

		number.Value = v
		m.FlattenHTTP(operation, numberPoint, toGenericParameters(number)...)
	}

	<-time.After(2 * time.Second)

	number.Value = expectedValue

	requestData := httpserver.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, []*structs.NumberPoint{number}, true)
}

// TestSendSum - tests the sum operation
func TestSendSum(t *testing.T) {

	testFlatOperation(t, timeline.Sum, 11, 5.5, 1.24, 3.76, 0.5)
}

// TestSendAvg - tests the avg operation
func TestSendAvg(t *testing.T) {

	testFlatOperation(t, timeline.Avg, 40, 25, 25, 25, 25, 100)
}

// TestSendMax - tests the max operation
func TestSendMax(t *testing.T) {

	testFlatOperation(t, timeline.Max, 10.8, 1, -200, 10.7, 10.8, 0, 5)
}

// TestSendMin - tests the min operation
func TestSendMin(t *testing.T) {

	testFlatOperation(t, timeline.Min, -200, 1, -200, 10.7, 10.8, 0)
}

// TestCountMin - tests the count operation
func TestCountMin(t *testing.T) {

	testFlatOperation(t, timeline.Count, 5, 1, -200, 10.7, 10.8, 0)
}
