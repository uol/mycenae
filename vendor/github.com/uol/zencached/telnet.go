package zencached

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/uol/logh"
)

//
// A persistent telnet connection to the memcached.
// @author rnojiri
//

type operation string

const (
	read  operation = "read"
	write operation = "write"
)

// Node - a memcached node
type Node struct {

	// Host - the server's hostname
	Host string

	// Port - the server's port
	Port int
}

// TelnetConfiguration - contains the telnet connection configuration
type TelnetConfiguration struct {

	// ReconnectionTimeout - the time duration between connection retries
	ReconnectionTimeout time.Duration

	// ReadWriteTimeout - the max time duration to wait a read or write operation
	ReadWriteTimeout time.Duration

	// MaxWriteRetries - the maximum number of write retries
	MaxWriteRetries int

	// ReadBufferSize - the size of the read buffer in bytes
	ReadBufferSize int
}

// Telnet - the telnet structure
type Telnet struct {
	address       *net.TCPAddr
	connection    *net.TCPConn
	logger        *logh.ContextualLogger
	configuration *TelnetConfiguration
	node          *Node
}

// NewTelnet - creates a new telnet connection
func NewTelnet(node *Node, configuration *TelnetConfiguration) (*Telnet, error) {

	if len(strings.TrimSpace(node.Host)) == 0 {
		return nil, fmt.Errorf("empty server host configured")
	}

	if node.Port <= 0 {
		return nil, fmt.Errorf("invalid server port configured")
	}

	t := &Telnet{
		logger:        logh.CreateContextualLogger("pkg", "zencached/telnet"),
		configuration: configuration,
		node:          node,
	}

	return t, nil
}

// resolveServerAddress - configures the server address
func (t *Telnet) resolveServerAddress() error {

	hostPort := fmt.Sprintf("%s:%d", t.node.Host, t.node.Port)

	if logh.DebugEnabled {
		t.logger.Debug().Msgf("resolving address: %s", hostPort)
	}

	var err error
	t.address, err = net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Err(err).Msgf("error resolving address: %s", hostPort)
		}
		return err
	}

	return nil
}

// Connect - try to Connect the telnet server
func (t *Telnet) Connect() error {

	err := t.resolveServerAddress()
	if err != nil {
		return err
	}

	err = t.dial()
	if err != nil {
		return err
	}

	if logh.InfoEnabled {
		t.logger.Info().Msg("connected!")
	}

	return nil
}

// dial - connects the telnet client
func (t *Telnet) dial() error {

	var err error
	t.connection, err = net.DialTCP("tcp", nil, t.address)
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Err(err).Msgf("error connecting to address: %s", t.address.String())
		}
		return err
	}

	err = t.connection.SetDeadline(time.Time{})
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Err(err).Msg("error setting connection's deadline")
		}
		return err
	}

	return nil
}

// Close - closes the active connection
func (t *Telnet) Close() {

	if t.connection == nil {
		return
	}

	err := t.connection.Close()
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Msg(err.Error())
		}
	}

	if logh.InfoEnabled {
		t.logger.Info().Msg("connection closed")
	}

	t.connection = nil
}

// Send - send some command to the server
func (t *Telnet) Send(command ...string) error {

	var err error
	for _, c := range command {
		for i := 0; i < t.configuration.MaxWriteRetries; i++ {
			if !t.writePayload(c) {
				t.Close()
				err = t.Connect()
				if err != nil {
					<-time.After(t.configuration.ReconnectionTimeout)
					continue
				}
			} else {
				break
			}
		}
	}

	return err
}

// Read - read the bytes from the connection, if any
func (t *Telnet) Read() ([]byte, error) {

	payload, err := t.readPayload()
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// writePayload - writes the payload
func (t *Telnet) writePayload(payload string) bool {

	if t.connection == nil {
		return false
	}

	err := t.connection.SetWriteDeadline(time.Now().Add(t.configuration.ReadWriteTimeout))
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Err(err).Msg("error setting write deadline")
		}
		return false
	}

	_, err = t.connection.Write([]byte(payload))
	if err != nil {
		t.logConnectionError(err, read)
		return false
	}

	return true
}

// readPayload - reads the payload from the connection
func (t *Telnet) readPayload() ([]byte, error) {

	err := t.connection.SetReadDeadline(time.Now().Add(t.configuration.ReadWriteTimeout))
	if err != nil {
		if logh.ErrorEnabled {
			t.logger.Error().Msg(fmt.Sprintf("error setting read deadline: %s", err.Error()))
		}
		return nil, err
	}

	readBuffer := make([]byte, t.configuration.ReadBufferSize)
	_, err = t.connection.Read(readBuffer)
	if err != nil {
		if castedErr, ok := err.(net.Error); ok && !castedErr.Timeout() {
			t.logConnectionError(err, read)
		}
		return nil, err
	}

	return readBuffer, nil
}

// logConnectionError - logs the connection error
func (t *Telnet) logConnectionError(err error, op operation) {

	if err == io.EOF {
		if logh.ErrorEnabled {
			t.logger.Error().Msg(fmt.Sprintf("[%s] connection EOF received, retrying connection...", op))
		}

		return
	}

	if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
		if logh.ErrorEnabled {
			t.logger.Error().Msg(fmt.Sprintf("[%s] connection timeout received, retrying connection...", op))
		}

		return
	}

	if logh.ErrorEnabled {
		t.logger.Error().Msg(fmt.Sprintf("[%s] error executing operation on connection: %s", op, err.Error()))
	}
}

// GetAddress - returns this node address
func (t *Telnet) GetAddress() string {
	return fmt.Sprintf("%s:%d", t.node.Host, t.node.Port)
}

// GetHost - returns this node host
func (t *Telnet) GetHost() string {
	return t.node.Host
}

// GetPort - returns this node port
func (t *Telnet) GetPort() int {
	return t.node.Port
}
