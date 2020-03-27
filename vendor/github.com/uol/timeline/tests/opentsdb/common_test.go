package timeline_opentsdb_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	port, err := strconv.Atoi(fmt.Sprintf("18%d", rand.Intn(999)))
	if err != nil {
		panic(err)
	}

	return port
}

// createOpenTSDBTransport - creates the http transport
func createOpenTSDBTransport() *timeline.OpenTSDBTransport {

	transportConf := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			BatchSendInterval:    1 * time.Second,
			RequestTimeout:       time.Second,
			SerializerBufferSize: 1024,
			TransportBufferSize:  5,
		},
		MaxReadTimeout:      3 * time.Second,
		ReconnectionTimeout: 1 * time.Second,
	}

	transport, err := timeline.NewOpenTSDBTransport(&transportConf)
	if err != nil {
		panic(err)
	}

	return transport
}

// listenTelnet - listens the telnet input
func listenTelnet(t *testing.T, c chan string, port int) {

	server, err := net.Listen("tcp", fmt.Sprintf("%s:%d", telnetHost, port))
	if err != nil {
		panic(err)
	}

	defer server.Close()

	conn, err := server.Accept()
	if err != nil {
		panic(err)
	}

	handleConnection(t, c, conn)

}

// handleConnection - handles the current connection
func handleConnection(t *testing.T, c chan string, conn net.Conn) {

	defer conn.Close()

	buffer := make([]byte, maxBuffer)

	err := conn.SetDeadline(time.Now().Add(100 * time.Second))
	if err != nil {
		panic(err)
	}

	n, err := conn.Read(buffer)
	if err != nil {
		panic(err)
	}

	if !assert.NotZero(t, n, "no characters found") {
		return
	}

	c <- (string)(bytes.Trim(buffer, "\x00"))
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
