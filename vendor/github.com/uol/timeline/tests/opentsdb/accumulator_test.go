package timeline_opentsdb_test

import (
	"math/rand"
	"strconv"
	"sync"
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

const defaultTransportSize int = 50

// createTimelineManagerA - creates a new timeline manager
func createTimelineManagerA(port, transportSize int) *timeline.Manager {

	backend := timeline.Backend{
		Host: telnetHost,
		Port: port,
	}

	transport := createOpenTSDBTransport(transportSize, 1*time.Second)

	conf := &timeline.DataTransformerConf{
		CycleDuration: funks.Duration{
			Duration: time.Millisecond * 900,
		},
		HashingAlgorithm: hashing.SHAKE256,
		HashSize:         12,
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

func genCustomHash() string {
	return "opentsdb-custom-hash-" + strconv.FormatInt(rand.Int63(), 10)
}

func testStorage(t *testing.T, customHash bool) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerA(port, defaultTransportSize)
	defer m.Shutdown()

	n := newArrayItem("storage", 0)

	hash, ok := storeNumber(t, time.Second, m, &n, customHash)
	if !assert.True(t, ok, "expected stored flag to be true") {
		return
	}

	assert.Truef(t, len(hash) > 0, "the generated hash length must be large than zero: %s", hash)

	incAccumulatedData(t, m, hash, 1)

	n.Value = 1

	testItemsAgainstReceivedLines(t, c, []serializer.ArrayItem{n})
}

// TestStorage - tests the hash storage operation
func TestStorage(t *testing.T) {

	testStorage(t, false)
}

// TestStorageCustomHash - tests the hash storage operation with custom hash
func TestStorageCustomHash(t *testing.T) {

	testStorage(t, true)
}

// storeNumber - stores a new number
func storeNumber(t *testing.T, ttl time.Duration, m *timeline.Manager, item *serializer.ArrayItem, customHash bool) (hash string, ok bool) {

	var err error

	if customHash {
		hash = genCustomHash()
		err = m.StoreHashedDataToAccumulateOpenTSDB(hash, ttl, item.Value, item.Timestamp, item.Metric, item.Tags...)
	} else {
		hash, err = m.StoreDataToAccumulateOpenTSDB(ttl, item.Value, item.Timestamp, item.Metric, item.Tags...)
	}

	if !assert.NoError(t, err, "error storing data") {
		return "", false
	}

	return hash, true
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
	point      serializer.ArrayItem
	number     uint64
	customHash bool
}

// testAdd - tests the add operation
func testAdd(t *testing.T, params ...accumParam) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerA(port, defaultTransportSize)
	defer m.Shutdown()

	expected := []serializer.ArrayItem{}

	for i := 0; i < len(params); i++ {

		hash, ok := storeNumber(t, time.Second, m, &params[i].point, params[i].customHash)
		if !assert.True(t, ok, "expected stored flag to be true") {
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

// TestAccumulateOneTypeOneTimeCustomHash - tests the add operation with custom hash
func TestAccumulateOneTypeOneTimeCustomHash(t *testing.T) {

	testAdd(t, accumParam{
		point:      newArrayItem("one-type-one-time", 0),
		number:     2,
		customHash: true,
	})
}

// TestAccumulateOneTypeMultipleTimes - tests the add operation
func TestAccumulateOneTypeMultipleTimes(t *testing.T) {

	testAdd(t, accumParam{
		point:  newArrayItem("one-type-mult-time", 0),
		number: 1 + uint64(rand.Int63n(50)),
	})
}

// TestAccumulateOneTypeMultipleTimesCustomHash - tests the add operation with custom hash
func TestAccumulateOneTypeMultipleTimesCustomHash(t *testing.T) {

	testAdd(t, accumParam{
		point:      newArrayItem("one-type-mult-time-hash", 0),
		number:     1 + uint64(rand.Int63n(50)),
		customHash: true,
	})
}

// buildAccumParameters - builds the accumulated parameters
func buildAccumParameters(initial, max int, times uint64, customHash bool) []accumParam {

	numParams := initial + rand.Intn(max)
	parameters := make([]accumParam, numParams)
	for i := 0; i < numParams; i++ {
		n := newArrayItem("accum-metric-"+strconv.Itoa(i), 0)
		parameters[i] = accumParam{
			point:  n,
			number: times,
		}

		if customHash {
			parameters[i].customHash = customHash
		}
	}

	return parameters
}

// TestAccumulateMultipleTypesOneTime - tests the add operation
func TestAccumulateMultipleTypesOneTime(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1, false)...)
}

// TestAccumulateMultipleTypesOneTimeCustomHash - tests the add operation with custom hash
func TestAccumulateMultipleTypesOneTimeCustomHash(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1, true)...)
}

// TestAccumulateMultipleTypesMultipleTimes - tests the add operation
func TestAccumulateMultipleTypesMultipleTimes(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1+uint64(rand.Int63n(50)), false)...)
}

// TestAccumulateMultipleTypesMultipleTimesCustomHash - tests the add operation with custom hash
func TestAccumulateMultipleTypesMultipleTimesCustomHash(t *testing.T) {

	testAdd(t, buildAccumParameters(1, 5, 1+uint64(rand.Int63n(50)), true)...)
}

func testDataTTL(t *testing.T, customHash bool) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerA(port, defaultTransportSize)
	defer m.Shutdown()

	metricPrefix := "metric"
	if customHash {
		metricPrefix += "-hashed"
	}

	n1 := newArrayItem(metricPrefix+"1", 0)
	n2 := newArrayItem(metricPrefix+"2", 0)

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, time.Second, m, &n1, customHash); !ok {
		return
	}

	if hash2, ok = storeNumber(t, time.Second, m, &n2, customHash); !ok {
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

// TestDataTTL - test storing data using ttl to expire it
func TestDataTTL(t *testing.T) {

	testDataTTL(t, false)
}

// TestDataTTLCustomHash - test storing data using ttl to expire it (with custom hash)
func TestDataTTLCustomHash(t *testing.T) {

	testDataTTL(t, true)
}

func testDataNoTTL(t *testing.T, customHash bool) {

	port := generatePort()

	c := make(chan string, 100)
	go listenTelnet(t, c, port, 1, time.Second)

	m := createTimelineManagerA(port, defaultTransportSize)
	defer m.Shutdown()

	metricPrefix := "metric"
	if customHash {
		metricPrefix += "-hashed-no-ttl"
	}

	n1 := newArrayItem(metricPrefix+"1", 0)
	n2 := newArrayItem(metricPrefix+"2", 0)

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, 0, m, &n1, customHash); !ok {
		return
	}

	if hash2, ok = storeNumber(t, time.Second, m, &n2, customHash); !ok {
		return
	}

	err := m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1") {
		return
	}

	<-time.After(2 * time.Second)

	err = m.IncrementAccumulatedData(hash2)
	if !assert.Equal(t, timeline.ErrNotStored, err, "expected hash2 to be expired") {
		return
	}

	err = m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1") {
		return
	}
}

// TestDataNoTTL - test storing data with no expiration ttl
func TestDataNoTTL(t *testing.T) {

	testDataNoTTL(t, false)
}

// TestDataNoTTLCustomHash - test storing data with no expiration ttl (with custom hash)
func TestDataNoTTLCustomHash(t *testing.T) {

	testDataNoTTL(t, true)
}
