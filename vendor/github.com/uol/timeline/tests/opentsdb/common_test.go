package timeline_opentsdb_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	serializer "github.com/uol/serializer/opentsdb"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

const (
	telnetHost = "localhost"
	maxBuffer  = 1024
)

// generatePort - generates a port
func generatePort() int {

	rand.Seed(time.Now().Unix())

	port, err := strconv.Atoi(fmt.Sprintf("1%d", rand.Intn(9999)))
	if err != nil {
		panic(err)
	}

	return port
}

// createOpenTSDBTransport - creates the http transport
func createOpenTSDBTransport(transportBufferSize int, batchSendInterval time.Duration) *timeline.OpenTSDBTransport {

	transportConf := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			BatchSendInterval: funks.Duration{
				Duration: batchSendInterval,
			},
			RequestTimeout: funks.Duration{
				Duration: time.Minute,
			},
			TransportBufferSize:  transportBufferSize,
			SerializerBufferSize: 1024,
		},
		ReadBufferSize: 64,
		MaxReadTimeout: funks.Duration{
			Duration: time.Millisecond,
		},
		ReconnectionTimeout: funks.Duration{
			Duration: time.Second,
		},
	}

	transport, err := timeline.NewOpenTSDBTransport(&transportConf)
	if err != nil {
		panic(err)
	}

	return transport
}

// listenTelnet - listens the telnet input
func listenTelnet(t *testing.T, c chan string, port, numRequests int, readTimeout time.Duration) {

	server, err := net.Listen("tcp", fmt.Sprintf("%s:%d", telnetHost, port))
	if err != nil {
		if strings.Contains(err.Error(), "address already in use") {
			<-time.After(time.Second)
			listenTelnet(t, c, generatePort(), numRequests, readTimeout)
		}
	}

	conn, err := server.Accept()
	if err != nil {
		panic(err)
	}

	handleConnection(t, c, conn, numRequests, readTimeout)

	server.Close()
}

// handleConnection - handles the current connection
func handleConnection(t *testing.T, c chan string, conn net.Conn, numRequests int, readTimeout time.Duration) {

	defer conn.Close()

	buffer := make([]byte, maxBuffer)

	err := conn.SetWriteDeadline(time.Time{})
	if err != nil {
		panic(err)
	}

	err = conn.SetReadDeadline(time.Now().Add(readTimeout))
	if err != nil {
		panic(err)
	}

	n := 0
	numReqOK := 0
	for i := 0; i < numRequests; i++ {
		n, err = conn.Read(buffer)
		if err != nil {
			if nErr, ok := err.(net.Error); ok {
				if nErr.Timeout() {
					break
				}
			}

			panic(err)
		}

		if !assert.Greater(t, n, 0, "no text received") {
			return
		}

		trimmed := bytes.Trim(buffer, "\x00")

		c <- (string)(trimmed)
		numReqOK++

		err = conn.SetReadDeadline(time.Now().Add(readTimeout))
		if err != nil {
			panic(err)
		}
	}

	if !assert.Equal(t, numRequests, numReqOK, "the number of requests differs") {
		return
	}
}

// newArrayItem - creates a new array item
func newArrayItem(metric string, value float64) serializer.ArrayItem {

	return serializer.ArrayItem{
		Metric:    metric,
		Timestamp: time.Now().Unix(),
		Value:     value,
		Tags: []interface{}{
			"tagk1", "tagv1",
			"tagk2", "tagv2",
			"tagk3", "tagv3",
		},
	}
}
