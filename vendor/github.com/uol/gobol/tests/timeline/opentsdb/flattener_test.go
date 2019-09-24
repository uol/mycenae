package timeline_opentsdb_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/hashing"
	"github.com/uol/gobol/timeline"
	serializer "github.com/uol/serializer/opentsdb"
	"go.uber.org/zap"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerF - creates a new timeline manager
func createTimelineManagerF(start bool, port int) *timeline.Manager {

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	backend := timeline.Backend{
		Host: telnetHost,
		Port: port,
	}

	transport := createOpenTSDBTransport(logger)

	conf := &timeline.FlattenerConfig{
		CycleDuration:    time.Millisecond * 900,
		HashingAlgorithm: hashing.SHA256,
	}

	flattener, err := timeline.NewFlattener(transport, conf, logger)
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

// testFlattenedValue - tests some inputed values
func testFlattenedValue(t *testing.T, c chan string, m *timeline.Manager, operation timeline.FlatOperation, items []serializer.ArrayItem, expectedItems []serializer.ArrayItem) {

	numItems := len(items)
	for i := 0; i < numItems; i++ {
		err := m.FlattenOpenTSDB(operation, items[i].Value, items[i].Timestamp, items[i].Metric, items[i].Tags...)
		if err != nil {
			panic(err)
		}
	}

	lines := <-c

	mainBuffer := strings.Builder{}

	for i := 0; i < len(expectedItems); i++ {

		tagsBuffer := strings.Builder{}

		for j := 0; j < len(expectedItems[i].Tags); j += 2 {
			tagsBuffer.WriteString(expectedItems[i].Tags[j].(string))
			tagsBuffer.WriteString("=")
			tagsBuffer.WriteString(expectedItems[i].Tags[j+1].(string))
			if j < len(expectedItems[i].Tags)-2 {
				tagsBuffer.WriteString(" ")
			}
		}

		mainBuffer.WriteString(fmt.Sprintf("put %s %d %.0f %s\n", expectedItems[i].Metric, expectedItems[i].Timestamp, expectedItems[i].Value, tagsBuffer.String()))
	}

	assert.Equal(t, mainBuffer.String(), lines, "lines does not match")
}

// TestSum - tests the sum operation
func TestSum(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManagerF(true, port)

	item := serializer.ArrayItem{
		Value:     10.0,
		Timestamp: time.Now().Unix(),
		Metric:    "sum",
		Tags: []interface{}{
			"ttl", "1",
			"ksid", "testksid",
		},
	}

	expected := item
	expected.Value = 30.0

	testFlattenedValue(t, c, m,
		timeline.Sum,
		[]serializer.ArrayItem{item, item, item},
		[]serializer.ArrayItem{expected},
	)
}

// TestAvg - tests the avg operation
func TestAvg(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManagerF(true, port)

	item1 := serializer.ArrayItem{
		Value:     10.0,
		Timestamp: time.Now().Unix(),
		Metric:    "avg",
		Tags: []interface{}{
			"ttl", "1",
			"ksid", "testksid",
		},
	}
	item2 := item1
	item2.Value = 4.0

	expected := item1
	expected.Value = 8.0

	testFlattenedValue(t, c, m,
		timeline.Avg,
		[]serializer.ArrayItem{item1, item1, item2},
		[]serializer.ArrayItem{expected},
	)
}

// TestMax - tests the max operation
func TestMax(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManagerF(true, port)

	item1 := serializer.ArrayItem{
		Value:     1.0,
		Timestamp: time.Now().Unix(),
		Metric:    "max",
		Tags: []interface{}{
			"ttl", "1",
			"ksid", "testksid",
		},
	}

	item2 := item1
	item2.Value = 4.0

	item3 := item1
	item3.Value = 7.0

	expected := item1
	expected.Value = 7.0

	testFlattenedValue(t, c, m,
		timeline.Max,
		[]serializer.ArrayItem{item1, item2, item3},
		[]serializer.ArrayItem{expected},
	)
}

// TestMin - tests the min operation
func TestMin(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManagerF(true, port)

	item1 := serializer.ArrayItem{
		Value:     1.0,
		Timestamp: time.Now().Unix(),
		Metric:    "min",
		Tags: []interface{}{
			"ttl", "1",
			"ksid", "testksid",
		},
	}

	item2 := item1
	item2.Value = 4.0

	item3 := item1
	item3.Value = 7.0

	expected := item1
	expected.Value = 1.0

	testFlattenedValue(t, c, m,
		timeline.Min,
		[]serializer.ArrayItem{item1, item2, item3},
		[]serializer.ArrayItem{expected},
	)
}

// TestCount - tests the count operation
func TestCount(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManagerF(true, port)

	item1 := serializer.ArrayItem{
		Value:     1.0,
		Timestamp: time.Now().Unix(),
		Metric:    "count",
		Tags: []interface{}{
			"ttl", "1",
			"ksid", "testksid",
		},
	}

	item2 := item1
	item2.Value = 4.0

	item3 := item1
	item3.Value = 7.0

	expected := item1
	expected.Value = 3.0

	testFlattenedValue(t, c, m,
		timeline.Count,
		[]serializer.ArrayItem{item1, item2, item3},
		[]serializer.ArrayItem{expected},
	)
}
