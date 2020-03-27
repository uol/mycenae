package timeline_opentsdb_test

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/hashing"
	serializer "github.com/uol/serializer/opentsdb"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerA - creates a new timeline manager
func createTimelineManagerA(port int, ttl time.Duration) *timeline.Manager {

	backend := timeline.Backend{
		Host: telnetHost,
		Port: port,
	}

	transport := createOpenTSDBTransport()

	conf := &timeline.AccumulatorConf{
		DataTransformerConf: timeline.DataTransformerConf{
			CycleDuration:    time.Millisecond * 900,
			HashingAlgorithm: hashing.SHAKE256,
			HashSize:         12,
		},
		DataTTL: ttl,
	}

	accumulator := timeline.NewAccumulator(conf)

	manager, err := timeline.NewManager(transport, nil, accumulator, &backend)
	if err != nil {
		panic(err)
	}

	err = manager.Start()
	if err != nil {
		panic(err)
	}

	return manager
}

// storeNumber - stores a new number
func storeNumber(t *testing.T, m *timeline.Manager, item *serializer.ArrayItem) (hash string, ok bool) {

	hash, err := m.StoreDataToAccumulateOpenTSDB(item.Value, item.Timestamp, item.Metric, item.Tags...)
	if !assert.NoError(t, err, "error storing data") {
		return "", false
	}

	return hash, true
}

// TestStorage - tests the hash storage operation
func TestStorage(t *testing.T) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port)

	m := createTimelineManagerA(port, time.Minute)
	defer m.Shutdown()

	n := newArrayItem("storage", 0)

	if hash, ok := storeNumber(t, m, &n); !ok {
		return
	} else {
		assert.Truef(t, len(hash) > 0, "the generated hash length must be large than zero: %s", hash)
	}
}

// incAccumulatedData - increments the accumulated data N times
func incAccumulatedData(t *testing.T, m *timeline.Manager, hash string, times int) {

	wg := sync.WaitGroup{}

	for i := 0; i < times; i++ {
		wg.Add(1)
		go func() {
			err := m.IncrementAccumulatedData(hash)
			wg.Done()
			assert.NoError(t, err, "error incrementing accumulated data")
		}()
	}

	wg.Wait()
}

type accumParam struct {
	point  serializer.ArrayItem
	number uint64
}

// testAdd - tests the add operation
func testAdd(t *testing.T, params ...accumParam) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port)

	m := createTimelineManagerA(port, time.Minute)
	defer m.Shutdown()

	expected := []serializer.ArrayItem{}

	for i := 0; i < len(params); i++ {

		var hash string
		var ok bool
		if hash, ok = storeNumber(t, m, &params[i].point); !ok {
			return
		}

		params[i].point.Value = float64(params[i].number)

		incAccumulatedData(t, m, hash, int(params[i].number))

		expected = append(expected, params[i].point)
	}

	testItemsAgainstReceivedLines(t, c, expected)
}

// TestAccumulateOneTypeOneTime - tests the add operation
func TestAccumulateOneTypeOneTime(t *testing.T) {

	testAdd(t, accumParam{
		point:  newArrayItem("one-type-one-time", 0),
		number: 1,
	})
}

// TestAccumulateOneTypeMultipleTimes - tests the add operation
func TestAccumulateOneTypeMultipleTimes(t *testing.T) {

	rand.Seed(time.Now().Unix())
	testAdd(t, accumParam{
		point:  newArrayItem("one-type-mult-time", 0),
		number: 1 + uint64(rand.Int63n(50)),
	})
}

// buildAccumParameters - builds the accumulated parameters
func buildAccumParameters(initial, max int, times uint64) []accumParam {

	rand.Seed(time.Now().Unix())
	numParams := initial + rand.Intn(max)
	parameters := make([]accumParam, numParams)
	for i := 0; i < numParams; i++ {
		n := newArrayItem("accum-metric-"+strconv.Itoa(i), 0)
		parameters[i] = accumParam{
			point:  n,
			number: times,
		}
	}

	return parameters
}

// TestAccumulateMultipleTypesOneTime - tests the add operation
func TestAccumulateMultipleTypesOneTime(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1)...)
}

// TestAccumulateMultipleTypesMultipleTimes - tests the add operation
func TestAccumulateMultipleTypesMultipleTimes(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1+uint64(rand.Int63n(50)))...)
}

// TestDataTTL - tests the data TTL
func TestDataTTL(t *testing.T) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port)

	m := createTimelineManagerA(port, time.Second)
	defer m.Shutdown()

	n1 := newArrayItem("metric1", 0)
	n2 := newArrayItem("metric2", 0)

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, m, &n1); !ok {
		return
	}

	if hash2, ok = storeNumber(t, m, &n2); !ok {
		return
	}

	err := m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1") {
		return
	}

	for i := 0; i < 3; i++ {

		err = m.IncrementAccumulatedData(hash2)
		if !assert.NoError(t, err, "error was not expected incrementing hash2") {
			return
		}

		<-time.After(time.Second)
	}

	err = m.IncrementAccumulatedData(hash1)
	if !assert.Equal(t, timeline.ErrNotStored, err, "expected hash1 to be expired") {
		return
	}

	err = m.IncrementAccumulatedData(hash2)
	if !assert.NoError(t, err, "error was not expected incrementing hash2") {
		return
	}
}
