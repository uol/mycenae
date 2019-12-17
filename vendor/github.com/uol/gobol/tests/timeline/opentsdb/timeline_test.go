package timeline_opentsdb_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uol/gobol/timeline"
	serializer "github.com/uol/serializer/opentsdb"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManager - creates a new timeline manager
func createTimelineManager(start bool, port int) *timeline.Manager {

	backend := timeline.Backend{
		Host: telnetHost,
		Port: port,
	}

	transport := createOpenTSDBTransport()

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

// testValue - tests some inputed value
func testValue(t *testing.T, c chan string, m *timeline.Manager, items ...serializer.ArrayItem) {

	numItems := len(items)
	for i := 0; i < numItems; i++ {
		err := m.SendOpenTSDB(items[i].Value, items[i].Timestamp, items[i].Metric, items[i].Tags...)
		if err != nil {
			panic(err)
		}
	}

	lines := <-c

	mainBuffer := strings.Builder{}

	for i := 0; i < numItems; i++ {

		tagsBuffer := strings.Builder{}

		for j := 0; j < len(items[i].Tags); j += 2 {
			tagsBuffer.WriteString(items[i].Tags[j].(string))
			tagsBuffer.WriteString("=")
			tagsBuffer.WriteString(items[i].Tags[j+1].(string))
			if j < len(items[i].Tags)-2 {
				tagsBuffer.WriteString(" ")
			}
		}

		mainBuffer.WriteString(fmt.Sprintf("put %s %d %.1f %s\n", items[i].Metric, items[i].Timestamp, items[i].Value, tagsBuffer.String()))
	}

	assert.Equal(t, mainBuffer.String(), lines, "lines does not match")
}

// TestSingleInput - tests a simple input
func TestSingleInput(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManager(true, port)

	testValue(t, c, m,
		serializer.ArrayItem{
			Value:     10.10,
			Timestamp: time.Now().Unix(),
			Metric:    "metric",
			Tags: []interface{}{
				"ttl", "1",
				"ksid", "testksid",
				"tagName", "tagValue",
			},
		},
	)
}

// TestMultiInput - tests a multi input
func TestMultiInput(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManager(true, port)

	testValue(t, c, m,
		serializer.ArrayItem{
			Value:     10.10,
			Timestamp: time.Now().Unix(),
			Metric:    "metric1",
			Tags: []interface{}{
				"ttl", "1",
				"ksid", "testksid2",
				"tagName", "tagValue1",
			},
		},
		serializer.ArrayItem{
			Value:     30.5,
			Timestamp: time.Now().Unix(),
			Metric:    "metric2",
			Tags: []interface{}{
				"ttl", "1",
				"ksid", "testksid",
				"tagName", "tagValue2",
			},
		},
		serializer.ArrayItem{
			Value:     -100.9,
			Timestamp: time.Now().Unix(),
			Metric:    "metric3",
			Tags: []interface{}{
				"ttl", "7",
				"ksid", "testksid2",
				"tagName", "tagValue3",
			},
		},
	)
}

// TestSerialization - tests configuring the opentsdb variables
func TestSerialization(t *testing.T) {

	port := generatePort()

	c := make(chan string, 3)
	go listenTelnet(t, c, port)

	m := createTimelineManager(false, port)
	defer m.Shutdown()

	value := rand.Float64()
	timestamp := rand.Int63()
	tags := fmt.Sprintf("tag1=val1 tagTime=%d ttl=1", timestamp)
	metric := "serializationMetric"

	expected := fmt.Sprintf("put %s %d %.17f %s\n", metric, timestamp, value, tags)

	serialized, err := m.SerializeOpenTSDB(value, timestamp, metric,
		"tag1", "val1",
		"tagTime", timestamp,
		"ttl", 1)

	if !assert.NoError(t, err, "no error expected when serializing opentsdb") {
		return
	}

	assert.Equal(t, expected, serialized, "serialization not matches")
}
