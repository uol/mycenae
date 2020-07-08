package timeline

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/uol/logh"
)

/**
* Controls the network connection behaviour for tcp or udp protocols.
* @author rnojiri
**/

// rwOp - read or write operation
type rwOp string

const (
	read               rwOp = "read"
	readConnClosed     rwOp = "read_conn_closed"
	write              rwOp = "write"
	writeConnClosed    rwOp = "write_conn_closed"
	defaultConnRetries int  = 3
)

// customNetworkBehaviour - defines the interface used to control some points of this connection manager
type customNetworkBehaviour interface {

	// read - read the connection
	read(conn net.Conn, logConnError func(error, rwOp)) bool

	// dial - dial using a specific protocol
	dial() (net.Conn, error)

	// getAddress - returns the address
	getAddress() net.Addr
}

type rawNetworkConnection struct {
	transportConfiguration *DefaultTransportConfig
	configuration          *TCPUDPTransportConfig
	loggers                *logh.ContextualLogger
	connection             net.Conn
	connected              bool
	custom                 customNetworkBehaviour
}

// panicRecovery - recovers from panic
func (t *rawNetworkConnection) panicRecovery() {

	if r := recover(); r != nil {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(r.(error)).Msg("error recovery")
		}
	}
}

// transferData - transfers the data to the backend throught this transport
func (t *rawNetworkConnection) transferData(payload string) error {

	defer t.panicRecovery()

	for i := 0; i < t.configuration.MaxReconnectionRetries; i++ {
		if !t.writePayload(payload) {
			t.closeConnection()
			t.retryConnect()
		} else {
			if t.configuration.DisconnectAfterWrites {
				if logh.DebugEnabled {
					t.loggers.Debug().Msg("disconnecting after successful write")
				}
				t.closeConnection()
			}
			break
		}
	}

	return nil
}

// writePayload - writes the payload
func (t *rawNetworkConnection) writePayload(payload string) bool {

	if !t.connected {
		if logh.InfoEnabled {
			t.loggers.Info().Msg("connection is not ready...")
		}
		return false
	}

	err := t.connection.SetWriteDeadline(time.Now().Add(t.transportConfiguration.RequestTimeout.Duration))
	if err != nil {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(err).Msg("error setting write deadline")
		}
		return false
	}

	n, err := t.connection.Write(([]byte)(payload))
	if err != nil {
		if err == io.EOF {
			t.logConnectionError(err, writeConnClosed)
			return false
		}

		t.logConnectionError(err, write)
		return false
	}

	if logh.DebugEnabled {
		logh.Debug().Msgf("%d bytes were written to the connection", n)
	}

	if !t.custom.read(t.connection, t.logConnectionError) {
		return false
	}

	err = t.connection.SetDeadline(time.Time{})
	if err != nil {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Msg("error setting connection's deadline")
		}
		return false
	}

	return true
}

// logConnectionError - logs the connection error
func (t *rawNetworkConnection) logConnectionError(err error, operation rwOp) {

	if err == io.EOF {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Msg(fmt.Sprintf("[%s] connection EOF received, retrying connection...", operation))
		}
		return
	}

	if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Msg(fmt.Sprintf("[%s] connection timeout received, retrying connection...", operation))
		}
		return
	}

	if logh.ErrorEnabled {
		ev := t.loggers.Error()
		if t.transportConfiguration.PrintStackOnError {
			ev = ev.Caller()
		}
		ev.Msg(fmt.Sprintf("[%s] error executing operation on connection: %s", operation, err.Error()))
	}
}

// closeConnection - closes the active connection
func (t *rawNetworkConnection) closeConnection() {

	if t.connection == nil {
		return
	}

	err := t.connection.Close()
	if err != nil {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(err).Msg("error closing connection")
		}
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg("connection closed")
	}

	t.connection = nil
	t.connected = false
}

// MatchType - checks if this transport implementation matches the given type
func (t *rawNetworkConnection) MatchType(tt transportType) bool {

	return tt == typeOpenTSDB
}

// retryConnect - connects the telnet client
func (t *rawNetworkConnection) retryConnect() {

	if logh.InfoEnabled {
		t.loggers.Info().Msgf("starting a new connection to: %s:", t.custom.getAddress().String())
	}

	t.connected = false

	for {

		if t.connect() {
			t.connected = true
			break
		}

		if logh.InfoEnabled {
			t.loggers.Info().Msgf("connection retry to \"%s\" in: %s", t.custom.getAddress().String(), t.configuration.ReconnectionTimeout.String())
		}

		<-time.After(t.configuration.ReconnectionTimeout.Duration)
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msgf("connected to: %s", t.custom.getAddress().String())
	}
}

// connect - connects the telnet client
func (t *rawNetworkConnection) connect() bool {

	if logh.InfoEnabled {
		t.loggers.Info().Msg(fmt.Sprintf("connecting to opentsdb telnet: %s:", t.custom.getAddress().String()))
	}

	var err error
	t.connection, err = t.custom.dial()
	if err != nil {
		if logh.ErrorEnabled {
			t.loggers.Info().Msg(fmt.Sprintf("error connecting to address: %s", t.custom.getAddress().String()))
		}
		return false
	}

	err = t.connection.SetDeadline(time.Time{})
	if err != nil {
		if logh.ErrorEnabled {
			ev := t.loggers.Error()
			if t.transportConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Msg("error setting connection's deadline")
		}
		t.closeConnection()
		return false
	}

	return true
}
