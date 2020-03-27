package telnetsrv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/logh"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/stats"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/utils"
)

//
// Implements a telnet server to input data
// author: rnojiri
//

const lineSeparator byte = 10

// Server - the telnet server struct
type Server struct {
	listenAddress            string
	listener                 net.Listener
	onErrorTimeout           time.Duration
	sendStatsTimeout         time.Duration
	maxIdleConnectionTimeout time.Duration
	maxBufferSize            int64
	collector                *collector.Collector
	logger                   *logh.ContextualLogger
	timelineManager          *stats.TimelineManager
	telnetHandler            TelnetDataHandler
	lineSplitter             []byte
	statsConnectionTags      []interface{}
	sharedConnectionCounter  *uint32
	numLocalConnections      uint32
	maxConnections           uint32
	denyNewConnections       uint32
	terminate                bool
	port                     string
	name                     string
	closeConnectionChannel   *chan struct{}
	connectedIPMap           sync.Map
	globalTelnetConfigs      *structs.GlobalTelnetServerConfiguration
}

// New - creates a new telnet server
func New(serverConfiguration *structs.TelnetServerConfiguration, globalTelnetConfigs *structs.GlobalTelnetServerConfiguration, sharedConnectionCounter *uint32, maxConnections uint32, closeConnectionChannel *chan struct{}, collector *collector.Collector, timelineManager *stats.TimelineManager, telnetHandler TelnetDataHandler) (*Server, error) {

	onErrorTimeoutDuration, err := time.ParseDuration(serverConfiguration.OnErrorTimeout)
	if err != nil {
		return nil, err
	}

	sendStatsTimeoutDuration, err := time.ParseDuration(serverConfiguration.SendStatsTimeout)
	if err != nil {
		return nil, err
	}

	maxIdleConnectionTimeoutDuration, err := time.ParseDuration(serverConfiguration.MaxIdleConnectionTimeout)
	if err != nil {
		return nil, err
	}

	strPort := fmt.Sprintf("%d", serverConfiguration.Port)

	return &Server{
		listenAddress:            fmt.Sprintf("%s:%d", serverConfiguration.Host, serverConfiguration.Port),
		onErrorTimeout:           onErrorTimeoutDuration,
		sendStatsTimeout:         sendStatsTimeoutDuration,
		maxIdleConnectionTimeout: maxIdleConnectionTimeoutDuration,
		maxBufferSize:            serverConfiguration.MaxBufferSize,
		collector:                collector,
		logger:                   logh.CreateContextualLogger(constants.StringsPKG, "telnetsrv"),
		timelineManager:          timelineManager,
		telnetHandler:            telnetHandler,
		lineSplitter:             []byte{lineSeparator},
		terminate:                false,
		port:                     strPort,
		sharedConnectionCounter:  sharedConnectionCounter,
		maxConnections:           maxConnections,
		denyNewConnections:       0,
		closeConnectionChannel:   closeConnectionChannel,
		name:                     serverConfiguration.ServerName,
		connectedIPMap:           sync.Map{},
		globalTelnetConfigs:      globalTelnetConfigs,
		statsConnectionTags: []interface{}{
			"type", "tcp",
			"port", strPort,
			"source", telnetHandler.SourceName(),
		},
	}, nil
}

// extractIP - extracts the remote address IP from the connection
func (server *Server) extractIP(conn net.Conn) string {

	array := strings.Split(conn.RemoteAddr().String(), ":")

	var remoteAddress string
	if len(array) > 0 {
		remoteAddress = array[0]
	}

	return remoteAddress
}

const cFuncListen string = "Listen"

// Listen - starts to listen and to handle the incoming messages
func (server *Server) Listen() error {

	var err error
	server.listener, err = net.Listen("tcp", server.listenAddress)
	if nil != err {
		return err
	}

	go server.collectStats()

	if logh.InfoEnabled {
		server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf("listening telnet connections at %q...", server.listener.Addr())
	}

	go func() {

		for {

			conn, err := server.listener.Accept()
			if err != nil {
				if utils.IsConnectionClosedError(err) {
					if logh.InfoEnabled {
						server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msg("connection was closed")
					}
					return
				}

				if logh.ErrorEnabled {
					server.logger.Error().Str(constants.StringsFunc, cFuncListen).Err(err).Send()
				}
				<-time.After(server.onErrorTimeout)
				continue
			}

			remoteAddressIP := server.extractIP(conn)

			// reports the connection IP and source
			server.statsNetworkIP(cFuncListen, remoteAddressIP)
			server.statsNetworkConnectionOpen(cFuncListen)

			if _, stored := server.connectedIPMap.LoadOrStore(remoteAddressIP, struct{}{}); stored {

				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf("telnet server will not accept new connections from %s", remoteAddressIP)
				}
				go server.closeConnection(conn, "multiple", false)

				continue
			}

			if atomic.LoadUint32(&server.denyNewConnections) == 1 {

				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf("telnet server is not accepting new connections, denying connection from %s", remoteAddressIP)
				}
				go server.closeConnection(conn, "deny", false)

				continue
			}

			if atomic.LoadUint32(server.sharedConnectionCounter) >= server.maxConnections {

				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf("max number of telnet connections reached (%d), closing connection from %s", server.maxConnections, remoteAddressIP)
				}
				go server.closeConnection(conn, "limit", false)

				continue
			}

			server.increaseCounter(server.sharedConnectionCounter)
			server.increaseCounter(&server.numLocalConnections)

			if !server.globalTelnetConfigs.SilenceLogs {
				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf("received new connection from %s", remoteAddressIP)
				}
			}

			err = conn.SetDeadline(time.Now().Add(server.maxIdleConnectionTimeout))
			if err != nil {
				go server.closeConnection(conn, "deadline", true)
				continue
			}

			go server.handleConnection(conn)
		}
	}()

	return nil
}

// handleConnection - handles an incoming connection
func (server *Server) handleConnection(conn net.Conn) {

	defer server.recover(conn, "handleConnection")

	startTime := time.Now()

	buffer := make([]byte, server.maxBufferSize)
	data := make([]byte, 0)
	var err error
	var n int
ConnLoop:
	for {
		select {
		case <-(*server.closeConnectionChannel):
			go server.closeConnection(conn, "balancing", true)
			break ConnLoop
		default:
		}

		err = conn.SetWriteDeadline(time.Now().Add(server.maxIdleConnectionTimeout))
		if err != nil {
			go server.closeConnection(conn, "wdeadline", true)
			break ConnLoop
		}

		_, err = conn.Write([]byte("OK" + string(server.lineSplitter)))
		if err != nil {
			if err == io.EOF {
				go server.closeConnection(conn, "weof", true)
				break ConnLoop
			}

			if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
				go server.closeConnection(conn, "wtimeout", true)
				break ConnLoop
			}

			go server.closeConnection(conn, "wunknown", true)
			break ConnLoop
		}

		err = conn.SetReadDeadline(time.Now().Add(server.maxIdleConnectionTimeout))
		if err != nil {
			go server.closeConnection(conn, "deadline", true)
			break ConnLoop
		}

		n, err = conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				go server.closeConnection(conn, "eof", true)
				break ConnLoop
			}

			if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
				go server.closeConnection(conn, "timeout", true)
				break ConnLoop
			}

			go server.closeConnection(conn, "unknown", true)
			break ConnLoop
		}

		if n == 0 {
			continue
		}

		data = append(data, buffer[0:n]...)

		if data[len(data)-1] == lineSeparator {
			byteLines := bytes.Split(data, server.lineSplitter)
			go func() {
				for _, byteLine := range byteLines {
					server.telnetHandler.Handle(string(byteLine))
				}
			}()
			data = make([]byte, 0)
		}
	}

	server.statsNetworkConnectionOpenTime(cFuncListen, startTime)

	if !server.globalTelnetConfigs.SilenceLogs {
		if err != nil {
			if logh.ErrorEnabled {
				server.logger.Error().Str(constants.StringsFunc, cFuncListen).Err(err).Msgf("connection loop was broken")
			}
		} else if logh.DebugEnabled {
			server.logger.Debug().Str(constants.StringsFunc, cFuncListen).Msg("connection loop was broken with no error")
		}
	}
}

// increaseCounter - increases the counter
func (server *Server) increaseCounter(num *uint32) uint32 {

	return atomic.AddUint32(num, 1)
}

// decreaseCounter - decreases the counter
func (server *Server) decreaseCounter(num *uint32) uint32 {

	return atomic.AddUint32(num, ^uint32(0))
}

const cFuncCloseConnection string = "closeConnection"

// closeConnection - closes an tcp connection
func (server *Server) closeConnection(conn net.Conn, reason string, subtractCounter bool) {

	startTime := time.Now()

	remoteAddressIP := server.extractIP(conn)

	err := conn.Close()
	if err != nil && !server.globalTelnetConfigs.SilenceLogs && logh.ErrorEnabled {
		server.logger.Error().Str(constants.StringsFunc, cFuncCloseConnection).Err(err).Msgf("error closing tcp telnet connection %s (%s): %s", remoteAddressIP, server.telnetHandler.SourceName())
	}

	conn = nil

	server.connectedIPMap.Delete(remoteAddressIP)

	server.statsNetworkConnectionClose(cFuncCloseConnection, reason)

	var localConns, sharedConns uint32

	if subtractCounter {
		localConns = server.decreaseCounter(&server.numLocalConnections)
		sharedConns = server.decreaseCounter(server.sharedConnectionCounter)
	} else {
		localConns = server.numLocalConnections
		sharedConns = *server.sharedConnectionCounter
	}

	if !server.globalTelnetConfigs.SilenceLogs {

		if logh.InfoEnabled {
			server.logger.Info().Str(constants.StringsFunc, cFuncCloseConnection).Msgf("tcp telnet connection closed %s (%s) from %d connections)", remoteAddressIP, reason, localConns)
			server.logger.Info().Str(constants.StringsFunc, cFuncCloseConnection).Msgf("total telnet connections: %d / %d (local conns / total conns -> %s)", localConns, sharedConns, server.telnetHandler.SourceName())
		}
	}

	server.statsNetworkConnectionCloseTime(cFuncCloseConnection, reason, startTime)
}

// Shutdown - stops listening
func (server *Server) Shutdown() error {

	server.terminate = true

	err := server.listener.Close()
	if err != nil {
		if logh.ErrorEnabled {
			server.logger.Error().Str(constants.StringsFunc, "Shutdown").Err(err).Send()
		}

		return err
	}

	if logh.InfoEnabled {
		server.logger.Info().Str(constants.StringsFunc, "Shutdown").Msg("telnet server have shutdown")
	}

	return nil
}

const (
	cFuncCollectStats string = "collectStats"
)

// collectStats - send all TCP connection statistics
func (server *Server) collectStats() {

	if logh.InfoEnabled {
		server.logger.Info().Str(constants.StringsFunc, cFuncCollectStats).Msg("starting telnet server stats")
	}

	for {
		<-time.After(server.sendStatsTimeout)

		if server.terminate {
			if logh.InfoEnabled {
				server.logger.Info().Str(constants.StringsFunc, cFuncCollectStats).Msg("terminating telnet server stats")
			}
			return
		}

		server.statsNetworkConnection(cFuncCollectStats)
	}
}

// recover - recovers from panic
func (server *Server) recover(conn net.Conn, function string) {

	if r := recover(); r != nil {
		if logh.ErrorEnabled {
			server.logger.Error().Msgf("panic recovery: %v", r)
		}
		server.decreaseCounter(&server.numLocalConnections)
		server.decreaseCounter(server.sharedConnectionCounter)
	}
}

// DenyNewConnections - deny new connections
func (server *Server) DenyNewConnections(deny bool) {

	var newValue uint32
	if deny {
		newValue = 1
	}

	atomic.SwapUint32(&server.denyNewConnections, newValue)
}

// GetName - returns the server's name
func (server *Server) GetName() string {

	return server.name
}
