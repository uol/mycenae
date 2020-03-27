package timeline_http_test

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gotest"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerA - creates a new timeline manager
func createTimelineManagerA() *timeline.Manager {

	backend := timeline.Backend{
		Host: gotest.TestServerHost,
		Port: gotest.TestServerPort,
	}

	transport := createHTTPTransport()

	conf := &timeline.AccumulatorConf{
		DataTransformerConf: timeline.DataTransformerConf{
			CycleDuration:    time.Millisecond * 900,
			HashingAlgorithm: hashing.SHAKE128,
			HashSize:         12,
		},
		DataTTL: time.Second * 1,
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
func storeNumber(t *testing.T, m *timeline.Manager, n *timeline.NumberPoint) (hash string, ok bool) {

	hash, err := m.StoreDataToAccumulateHTTP(numberPoint, toGenericParameters(n)...)
	if !assert.NoError(t, err, "error storing data") {
		return "", false
	}

	return hash, true
}

// TestStorage - tests the hash storage operation
func TestStorage(t *testing.T) {

	m := createTimelineManagerA()
	defer m.Shutdown()

	n := newNumberPoint(0)

	if hash, ok := storeNumber(t, m, n); !ok {
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
	point  *timeline.NumberPoint
	number uint64
}

// testAdd - tests the add operation
func testAdd(t *testing.T, params ...accumParam) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerA()
	defer m.Shutdown()

	expected := []*timeline.NumberPoint{}

	for i := 0; i < len(params); i++ {

		var hash string
		var ok bool
		if hash, ok = storeNumber(t, m, params[i].point); !ok {
			return
		}

		params[i].point.Value = float64(params[i].number)

		incAccumulatedData(t, m, hash, int(params[i].number))

		expected = append(expected, params[i].point)
	}

	<-time.After(2 * time.Second)

	requestData := gotest.WaitForHTTPServerRequest(s)
	testRequestData(t, requestData, expected, true, true)
}

// TestAccumulateOneTypeOneTime - tests the add operation
func TestAccumulateOneTypeOneTime(t *testing.T) {

	testAdd(t, accumParam{
		point:  newNumberPoint(0),
		number: 1,
	})
}

// TestAccumulateOneTypeMultipleTimes - tests the add operation
func TestAccumulateOneTypeMultipleTimes(t *testing.T) {

	rand.Seed(time.Now().Unix())
	testAdd(t, accumParam{
		point:  newNumberPoint(0),
		number: 100 + uint64(rand.Int63n(5000)),
	})
}

// buildAccumParameters - builds the accumulated parameters
func buildAccumParameters(initial, max int, times uint64) []accumParam {

	rand.Seed(time.Now().Unix())
	numParams := initial + rand.Intn(max)
	parameters := make([]accumParam, numParams)
	for i := 0; i < numParams; i++ {
		n := newNumberPoint(0)
		n.Metric = n.Metric + strconv.Itoa(i)
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

	testAdd(t, buildAccumParameters(1, 5, 2+uint64(rand.Int63n(5)))...)
}

// TestDataTTL - tests the data TTL
func TestDataTTL(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerA()
	defer m.Shutdown()

	n1 := newNumberPoint(0)
	n2 := newNumberPoint(0)
	n2.Metric = "updated"

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, m, n1); !ok {
		return
	}

	if hash2, ok = storeNumber(t, m, n2); !ok {
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

	<-time.After(3 * time.Second)
}
