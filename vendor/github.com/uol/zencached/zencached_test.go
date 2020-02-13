package zencached_test

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/zencached"
)

//
// These tests requires a local memcached to run.
// See the "createZencached" function, it instantiates a memcached cluster using docker.
// author: rnojiri
//

var numNodes int
var defaultTTL []byte = []byte("60")

// createZencached - creates a new client
func createZencached(metricsCollector zencached.MetricsCollector) *zencached.Zencached {

	c := &zencached.Configuration{
		Nodes:                 setupMemcachedDocker(),
		NumConnectionsPerNode: 3,
		TelnetConfiguration:   *createTelnetConf(),
	}

	numNodes = len(c.Nodes)

	z, err := zencached.New(c, metricsCollector)
	if err != nil {
		panic(err)
	}

	return z
}

// TestCreateZen - tests creating and closing the client
func TestCreateZen(t *testing.T) {
	z := createZencached(nil)
	z.Shutdown()
}

// TestRouting - tests the routing algorithm
func TestRouting(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	f := func(key []byte, expected int) bool {

		tconn, index := z.GetTelnetConnection(key, key)
		if !assert.Equalf(t, expected, index, "expected index %d", expected) {
			return false
		}

		z.ReturnTelnetConnection(tconn, index)

		return true
	}

	if !f([]byte{0, 1, 2, 255}, 0) { //should be index 0
		return
	}

	if !f([]byte{10, 199, 202, 149}, 2) { //should be index 2
		return
	}

	if !f([]byte{206, 98, 60, 4}, 1) { //should be index 1
		return
	}

	if !f([]byte{206, 98, 60, 3}, 0) { //should be index 0
		return
	}
}

// TestNodePool - tests the node pool
func TestNodePool(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	waitTime := 50 * time.Millisecond

	f := func(minExpectedWait, maxExpectedWait time.Duration, testNumber int) {

		start := time.Now()
		tconn, index := z.GetTelnetConnection([]byte{44, 11, 89, 4}, nil)

		totalDuration := time.Since(start)

		assert.Truef(t, totalDuration.Milliseconds() >= minExpectedWait.Milliseconds() && totalDuration.Milliseconds() < maxExpectedWait.Milliseconds(), "wrong duration for test %d: %d", testNumber, totalDuration.Milliseconds())

		<-time.After(waitTime)

		z.ReturnTelnetConnection(tconn, index)
	}

	go f(0*time.Millisecond, 1*time.Millisecond, 1)
	go f(0*time.Millisecond, 1*time.Millisecond, 2)
	go f(0*time.Millisecond, 1*time.Millisecond, 3)
	go f(waitTime, waitTime+(1*time.Millisecond), 4)
}

// TestAddCommand - tests the add command
func TestAddCommand(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	f := func(route []byte, key, value string, expectedStored bool, testIndex int) {

		stored, err := z.Storage(zencached.Add, route, []byte(key), []byte(value), defaultTTL)
		if err != nil {
			panic(err)
		}

		assert.Truef(t, expectedStored == stored, "unexpected storage status for test %d and key %s", testIndex, key)
	}

	f([]byte{3}, "test1", "test1", true, 1)
	f([]byte{3}, "test2", "test2", true, 1)
	f([]byte{9}, "test1", "error", false, 1)

	f([]byte{4}, "test1", "test1", true, 2)
	f([]byte{4}, "test2", "test2", true, 2)
	f([]byte{7}, "test1", "error", false, 2)

	f([]byte{5}, "test1", "test1", true, 3)
	f([]byte{5}, "test2", "test2", true, 3)
	f([]byte{8}, "test1", "error", false, 3)
}

// rawSetKey - sets a key on memcached using raw command
func rawSetKey(telnetConn *zencached.Telnet, key, value string) {

	err := telnetConn.Send([]byte(fmt.Sprintf("set %s 0 %d %d\r\n%s\r\n", key, 60, len(value), value)))
	if err != nil {
		panic(err)
	}

	_, err = telnetConn.Read([][]byte{[]byte("STORED")})
	if err != nil {
		panic(err)
	}
}

// TestGetCommand - tests the get command
func TestGetCommand(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	f := func(route []byte, key, value string, testIndex int) {

		telnetConn, index := z.GetTelnetConnection(route, []byte(key))
		defer z.ReturnTelnetConnection(telnetConn, index)

		rawSetKey(telnetConn, key, value)

		response, found, err := z.Get(route, []byte(key))
		if err != nil {
			panic(err)
		}

		if !assert.Truef(t, found, "expected value from key \"%s\" to be found on test %d", key, testIndex) {
			return
		}

		assert.Equal(t, []byte(value), response, "expected values to be equal")
	}

	f([]byte{3}, "test1", "test1", 1)
	f([]byte{3}, "test2", "test2", 2)
	f([]byte{9}, "test1", "test3", 3)
	f([]byte{4}, "test4", "test4", 4)
	f([]byte{4}, "test5", "test5", 5)
	f([]byte{7}, "test5", "test6", 6)
	f([]byte{5}, "test7", "test7", 7)
	f([]byte{5}, "test8", "test8", 8)
	f([]byte{8}, "test7", "test8", 9)
}

// TestSetCommand - tests the set command
func TestSetCommand(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	f := func(route []byte, key, value string, testIndex int) {

		stored, err := z.Storage(zencached.Set, route, []byte(key), []byte(value), defaultTTL)
		if err != nil {
			panic(err)
		}

		if !assert.Truef(t, stored, "unexpected storage status for test %d", testIndex, key) {
			return
		}

		storedValue, found, err := z.Get(route, []byte(key))

		if !assert.Truef(t, found, "unexpected get status for test %d", testIndex, key) {
			return
		}

		assert.Equal(t, []byte(value), storedValue, "expected the same values")
	}

	f([]byte{3}, "test1", "test1", 1)
	f([]byte{3}, "test2", "test2", 2)
	f([]byte{9}, "test1", "test3", 3)
	f([]byte{4}, "test4", "test4", 4)
	f([]byte{4}, "test5", "test5", 5)
	f([]byte{7}, "test5", "test6", 6)
	f([]byte{5}, "test7", "test7", 7)
	f([]byte{5}, "test8", "test8", 8)
	f([]byte{8}, "test7", "test8", 9)
}

// TestDeleteCommand - tests the delete command
func TestDeleteCommand(t *testing.T) {

	z := createZencached(nil)
	defer z.Shutdown()

	f := func(route []byte, key, value string, setValue bool, testIndex int) {

		telnetConn, index := z.GetTelnetConnection(route, []byte(key))
		defer z.ReturnTelnetConnection(telnetConn, index)

		if setValue {
			rawSetKey(telnetConn, key, value)
		}

		status, err := z.Delete(route, []byte(key))
		if err != nil {
			panic(err)
		}

		if !assert.Truef(t, status == setValue, "unexpected delete status for test %d", testIndex, key) {
			return
		}

		if setValue {
			_, found, err := z.Get(route, []byte(key))
			if err != nil {
				panic(err)
			}

			if !assert.Truef(t, !found, "unexpected get status for test %d", testIndex, key) {
				return
			}
		}
	}

	f([]byte{3}, "test1", "test1", true, 1)
	f([]byte{3}, "test2", "test2", true, 2)
	f([]byte{9}, "test1", "test3", false, 3)
	f([]byte{4}, "test4", "test4", true, 4)
	f([]byte{4}, "test5", "test5", true, 5)
	f([]byte{7}, "test5", "test6", false, 6)
	f([]byte{5}, "test7", "test7", true, 7)
	f([]byte{5}, "test8", "test8", true, 8)
	f([]byte{8}, "test7", "test8", false, 9)
}

type testCollector struct {
	collected []string
}

func (c *testCollector) Count(metric string, value float64, tags ...string) {
	c.collected = append(c.collected, fmt.Sprintf("count/%s/%f/%v", metric, value, tags))
}

func (c *testCollector) Maximum(metric string, value float64, tags ...string) {
	c.collected = append(c.collected, fmt.Sprintf("max/%s/%f/%v", metric, value, tags))
}

// TestMetricsCollector - tests the metrics collector interface
func TestMetricsCollector(t *testing.T) {

	tc := testCollector{
		collected: []string{},
	}

	z := createZencached(&tc)
	defer z.Shutdown()

	createValidations := func(operation string) []*regexp.Regexp {
		return []*regexp.Regexp{
			regexp.MustCompile("count/zencached\\.node\\.distribution\\.count/[0-9\\.]+/\\[node [0-9\\.]+\\]"),
			regexp.MustCompile(fmt.Sprintf("count/zencached\\.operation\\.count/[0-9\\.]+/\\[node [0-9\\.]+\\ operation %s\\]", operation)),
			regexp.MustCompile("max/zencached\\.node\\.conn\\.available\\.time/[0-9\\.]+/\\[node [0-9\\.]+\\]"),
			regexp.MustCompile(fmt.Sprintf("max/zencached\\.operation\\.time/[0-9\\.]+/\\[node [0-9\\.]+ operation %s\\]", operation)),
			regexp.MustCompile(fmt.Sprintf("count/zencached\\.cache\\.(hit|miss)/[0-9\\.]+/\\[node [0-9\\.]+ operation %s\\]", operation)),
		}
	}

	validateArray := func(operation string) {

		<-time.After(300 * time.Millisecond)

		validations := createValidations(operation)

		assert.Truef(t, len(validations) == len(tc.collected), "the number of inputs are different: %d - %v", len(tc.collected), tc.collected)

		for i := 0; i < len(tc.collected); i++ {
			var matched bool
			for j := 0; j < len(validations); j++ {
				if validations[j].MatchString(tc.collected[i]) {
					matched = true
					break
				}
			}
			assert.Truef(t, matched, "no validation matches found for metrics: %v - %v", tc.collected, validations)
		}

		tc.collected = []string{}
	}

	key := []byte("key")
	value := []byte("value")

	_, err := z.Storage(zencached.Set, []byte{7}, key, value, defaultTTL)
	if err != nil {
		panic(err)
	}

	validateArray("set")

	_, err = z.Storage(zencached.Add, []byte{8}, key, value, defaultTTL)
	if err != nil {
		panic(err)
	}

	validateArray("add")

	_, err = z.Delete([]byte{3}, key)
	if err != nil {
		panic(err)
	}

	validateArray("delete")

	_, _, err = z.Get([]byte{10}, key)
	if err != nil {
		panic(err)
	}

	validateArray("get")
}
