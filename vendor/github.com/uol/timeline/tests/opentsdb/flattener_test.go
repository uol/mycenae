package timeline_opentsdb_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/hashing"
	serializer "github.com/uol/serializer/opentsdb"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerF - creates a new timeline manager
func createTimelineManagerF(start bool, port, transportSize int) *timeline.Manager {

	backend := timeline.Backend{
		Host: telnetHost,
		Port: port,
	}

	transport := createOpenTSDBTransport(transportSize, 1*time.Second)

	conf := &timeline.DataTransformerConf{
		CycleDuration: funks.Duration{
			Duration: time.Millisecond * 900,
		},
		HashingAlgorithm: hashing.SHA256,
	}

	flattener := timeline.NewFlattener(conf)

	manager, err := timeline.NewManager(transport, flattener, nil, &backend)
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

// buildOpenTSDBCmd - build the opentsdb commands
func buildOpenTSDBCmd(items []serializer.ArrayItem) []string {

	lines := make([]string, len(items))

	for i := 0; i < len(items); i++ {

		tagsBuffer := strings.Builder{}

		for j := 0; j < len(items[i].Tags); j += 2 {
			tagsBuffer.WriteString(items[i].Tags[j].(string))
			tagsBuffer.WriteString("=")
			tagsBuffer.WriteString(items[i].Tags[j+1].(string))
			if j < len(items[i].Tags)-2 {
				tagsBuffer.WriteString(" ")
			}
		}

		lines[i] = fmt.Sprintf("put %s %d %.0f %s\n", items[i].Metric, items[i].Timestamp, items[i].Value, tagsBuffer.String())
	}

	sort.Strings(lines)

	return lines
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

	testItemsAgainstReceivedLines(t, c, expectedItems)
}

// testItemsAgainstReceivedLines - test the expected items againt the received lines
func testItemsAgainstReceivedLines(t *testing.T, c chan string, expectedItems []serializer.ArrayItem) {

	expectedLines := buildOpenTSDBCmd(expectedItems)

	receivedText := <-c

	split := strings.Split(receivedText, "\n")
	lines := []string{}

	for i := 0; i < len(split); i++ {
		line := strings.TrimSpace(split[i])
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}

	if !assert.Truef(t, len(expectedItems) == len(lines), "number of lines not match: %d != %d", len(expectedItems), len(lines)) {
		return
	}

	sort.Strings(lines)

	for i := 0; i < len(expectedItems); i++ {

		if !compareOpenTSDBCmd(t, expectedLines[i], lines[i]) {
			return
		}
	}
}

// TestSum - tests the sum operation
func TestSum(t *testing.T) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerF(true, port, defaultTransportSize)
	defer m.Shutdown()

	item := newArrayItem("sum", 10.0)

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

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerF(true, port, defaultTransportSize)

	item1 := newArrayItem("avg", 10.0)

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

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerF(true, port, defaultTransportSize)

	item1 := newArrayItem("max", 1.0)

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

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerF(true, port, defaultTransportSize)

	item1 := newArrayItem("min", 1.0)

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

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerF(true, port, defaultTransportSize)

	item1 := newArrayItem("count", 1.0)

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
