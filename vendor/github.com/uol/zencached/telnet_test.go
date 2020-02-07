package zencached_test

import (
	"math/rand"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uol/zencached"
)

//
// Requires a local memcached.
// author: rnojiri
//

// setupMemcachedDocker - setup the nodes and returns the addresses
func setupMemcachedDocker() []zencached.Node {

	scriptPath := path.Join(os.Getenv("GOPATH"), "src", "github.com", "uol", "zencached", "setup_memcached.sh")
	out, err := exec.Command(scriptPath).Output()
	if err != nil {
		panic(err)
	}

	lines := strings.Split(string(out), "\n")
	lastLine := lines[len(lines)-1]

	hosts := strings.Split(lastLine, ",")
	nodes := make([]zencached.Node, len(hosts))

	for i, host := range hosts {

		nodes[i] = zencached.Node{
			Host: host,
			Port: 11211,
		}
	}

	return nodes
}

// createTelnetConf - creates a new telnet configuration
func createTelnetConf() *zencached.TelnetConfiguration {

	return &zencached.TelnetConfiguration{
		ReconnectionTimeout: 30 * time.Second,
		ReadWriteTimeout:    3 * time.Second,
		MaxWriteRetries:     3,
		ReadBufferSize:      255,
	}
}

// createTelnetTest - creates a new telnet client
func createTelnetTest(t *testing.T) *zencached.Telnet {

	nodes := setupMemcachedDocker()

	telnet, err := zencached.NewTelnet(&nodes[rand.Intn(len(nodes))], createTelnetConf())
	if err != nil {
		panic(err)
	}

	return telnet
}

// TestConnectionOpenClose - tests the open and close
func TestConnectionOpenClose(t *testing.T) {

	telnet := createTelnetTest(t)
	err := telnet.Connect()
	if !assert.NoError(t, err, "error connecting") {
		return
	}

	telnet.Close()
	if !assert.NoError(t, err, "error closing connection") {
		return
	}
}

// TestInfoCommand - tests a simple info command
func TestInfoCommand(t *testing.T) {

	telnet := createTelnetTest(t)

	defer telnet.Close()

	err := telnet.Send("stats\r\n")
	if !assert.NoError(t, err, "error sending command") {
		return
	}

	payload, err := telnet.Read()
	if !assert.NoError(t, err, "error reading response") {
		return
	}

	assert.True(t, regexp.MustCompile("STAT version [0-9\\.]+").MatchString(string(payload)), "version not found")
}

// TestInsertCommand - tests a simple insert command
func TestInsertCommand(t *testing.T) {

	telnet := createTelnetTest(t)

	defer telnet.Close()

	err := telnet.Send("add gotest 0 10 5\r\n", "value\r\n")
	if !assert.NoError(t, err, "error sending set command") {
		return
	}

	payload, err := telnet.Read()
	if !assert.NoError(t, err, "error reading response") {
		return
	}

	assert.True(t, strings.Contains(string(payload), "STORED"), "expected \"STORED\" as answer")

	err = telnet.Send("get gotest\r\n")
	if !assert.NoError(t, err, "error sending get command") {
		return
	}

	payload, err = telnet.Read()
	if !assert.NoError(t, err, "error reading response") {
		return
	}

	assert.True(t, strings.Contains(string(payload), "value"), "expected \"value\" to be stored")
}
