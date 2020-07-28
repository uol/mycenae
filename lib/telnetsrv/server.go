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
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/utils"
	tlmanager "github.com/uol/timelinemanager"
)

//
// Implements a telnet server to input data
// author: rnojiri
//

const lineSeparator byte = 10

type connCloseReason string

var (
	lineSplitter = []byte{lineSeparator}
	okResponse   = []byte("OK" + string(lineSplitter))
)

const (
	ccrMultiple  connCloseReason = "multiple"
	ccrDeny      connCloseReason = "deny"
	ccrLimit     connCloseReason = "limit"
	ccrDeadline  connCloseReason = "deadline"
	ccrBalancing connCloseReason = "balancing"
	ccrWDeadline connCloseReason = "wdeadline"
	ccrWEOF      connCloseReason = "weof"
	ccrWTimeout  connCloseReason = "wtimeout"
	ccrWUnknown  connCloseReason = "wunknown"
	ccrEOF       connCloseReason = "eof"
	ccrTimeout   connCloseReason = "timeout"
	ccrUnknown   connCloseReason = "unknown"
)

// Server - the telnet server struct
type Server struct {
	listenAddress                    string
	listener                         net.Listener
	maxBufferSize                    int64
	collector                        *collector.Collector
	logger                           *logh.ContextualLogger
	timelineManager                  *tlmanager.Instance
	telnetHandler                    TelnetDataHandler
	statsConnectionTags              []interface{}
	sharedConnectionCounter          *uint32
	numLocalConnections              uint32
	maxConnections                   uint32
	denyNewConnections               uint32
	terminate                        bool
	port                             string
	name                             string
	closeConnectionChannel           *chan struct{}
	connectedIPMap                   sync.Map
	multipleConnsAllowedHostsMap     map[string]bool
	globalTelnetConfiguration        *structs.TelnetManagerConfiguration
	telnetServerConfiguration        *structs.TelnetServerConfiguration
	hashMetricTelnetCommandCount     string
	hashMetricTelnetCommandFailures  string
	hashMetricTelnetCommandSuccesses string
}

// New - creates a new telnet server
func New(telnetServerConfiguration *structs.TelnetServerConfiguration, globalTelnetConfiguration *structs.TelnetManagerConfiguration, sharedConnectionCounter *uint32, maxConnections uint32, closeConnectionChannel *chan struct{}, collector *collector.Collector, timelineManager *tlmanager.Instance, telnetHandler TelnetDataHandler) (*Server, error) {

	logger := logh.CreateContextualLogger(constants.StringsPKG, "telnetsrv")

	multipleConnsAllowedHostsMap := map[string]bool{}

	if len(telnetServerConfiguration.MultipleConnsAllowedHosts) > 0 {

		for _, host := range telnetServerConfiguration.MultipleConnsAllowedHosts {

			multipleConnsAllowedHostsMap[host] = true

			if logh.InfoEnabled {
				logger.Info().Msgf(`allowing host "%s" to connect multiple times`, host)
			}
		}
	}

	strPort := fmt.Sprintf("%d", telnetServerConfiguration.Port)

	return &Server{
		listenAddress:                fmt.Sprintf("%s:%d", telnetServerConfiguration.Host, telnetServerConfiguration.Port),
		maxBufferSize:                telnetServerConfiguration.MaxBufferSize,
		collector:                    collector,
		logger:                       logger,
		timelineManager:              timelineManager,
		telnetHandler:                telnetHandler,
		terminate:                    false,
		port:                         strPort,
		sharedConnectionCounter:      sharedConnectionCounter,
		maxConnections:               maxConnections,
		denyNewConnections:           0,
		closeConnectionChannel:       closeConnectionChannel,
		name:                         telnetServerConfiguration.ServerName,
		connectedIPMap:               sync.Map{},
		globalTelnetConfiguration:    globalTelnetConfiguration,
		telnetServerConfiguration:    telnetServerConfiguration,
		multipleConnsAllowedHostsMap: multipleConnsAllowedHostsMap,
		statsConnectionTags: []interface{}{
			"type", "tcp",
			"port", strPort,
			"source", telnetHandler.GetSourceType().Name,
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

const (
	cFuncListen                string = "Listen"
	cMsgfSpecificDenyNewConns  string = "telnet server will not accept new connections from %s"
	cMsgfGeneralDenyNewConns   string = "telnet server is not accepting new connections, denying connection from %s"
	cMsgfMaxConnsReached       string = "max number of telnet connections reached (%d), closing connection from %s"
	cMsgfReceivingNewConn      string = "received new connection from %s"
	cMsgConnClosed             string = "connection was closed"
	cMsgfAllowingMultipleConns string = "telnet server will accept multiple connections from %s"
)

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
						server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msg(cMsgConnClosed)
					}
					return
				}

				if logh.ErrorEnabled {
					server.logger.Error().Str(constants.StringsFunc, cFuncListen).Err(err).Send()
				}

				continue
			}

			remoteAddressIP := server.extractIP(conn)

			// reports the connection IP and source
			server.statsNetworkIP(cFuncListen, remoteAddressIP)
			server.statsNetworkConnectionOpen(cFuncListen)

			if !server.telnetServerConfiguration.RemoveMultipleConnsRestriction {

				if _, stored := server.connectedIPMap.LoadOrStore(remoteAddressIP, struct{}{}); stored {

					if _, ok := server.multipleConnsAllowedHostsMap[remoteAddressIP]; !ok {

						if logh.InfoEnabled {
							server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf(cMsgfSpecificDenyNewConns, remoteAddressIP)
						}

						go server.closeConnection(conn, ccrMultiple, false)

						continue
					}

					if logh.InfoEnabled {
						server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf(cMsgfAllowingMultipleConns, remoteAddressIP)
					}
				}
			}

			if atomic.LoadUint32(&server.denyNewConnections) == 1 {

				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf(cMsgfGeneralDenyNewConns, remoteAddressIP)
				}
				go server.closeConnection(conn, ccrDeny, false)

				continue
			}

			if atomic.LoadUint32(server.sharedConnectionCounter) >= server.maxConnections {

				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf(cMsgfMaxConnsReached, server.maxConnections, remoteAddressIP)
				}
				go server.closeConnection(conn, ccrLimit, false)

				continue
			}

			server.increaseCounter(server.sharedConnectionCounter)
			server.increaseCounter(&server.numLocalConnections)

			if !server.telnetServerConfiguration.SilenceLogs {
				if logh.InfoEnabled {
					server.logger.Info().Str(constants.StringsFunc, cFuncListen).Msgf(cMsgfReceivingNewConn, remoteAddressIP)
				}
			}

			err = conn.SetDeadline(time.Now().Add(server.telnetServerConfiguration.MaxIdleConnectionTimeout.Duration))
			if err != nil {
				go server.closeConnection(conn, ccrDeadline, true)
				continue
			}

			go server.handleConnection(conn, remoteAddressIP)
		}
	}()

	return nil
}

// handleConnection - handles an incoming connection
func (server *Server) handleConnection(conn net.Conn, ip string) {

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
			go server.closeConnection(conn, ccrBalancing, true)
			break ConnLoop
		default:
		}

		err = conn.SetWriteDeadline(time.Now().Add(server.telnetServerConfiguration.MaxIdleConnectionTimeout.Duration))
		if err != nil {
			go server.closeConnection(conn, ccrWDeadline, true)
			break ConnLoop
		}

		_, err = conn.Write(okResponse)
		if err != nil {
			if err == io.EOF {
				go server.closeConnection(conn, ccrWEOF, true)
				break ConnLoop
			}

			if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
				go server.closeConnection(conn, ccrWTimeout, true)
				break ConnLoop
			}

			go server.closeConnection(conn, ccrWUnknown, true)
			if !server.telnetServerConfiguration.SilenceLogs && logh.WarnEnabled {
				server.logger.Warn().Err(err).Msg("telnet connection write: unexpected error")
			}
			break ConnLoop
		}

		err = conn.SetReadDeadline(time.Now().Add(server.telnetServerConfiguration.MaxIdleConnectionTimeout.Duration))
		if err != nil {
			go server.closeConnection(conn, ccrDeadline, true)
			break ConnLoop
		}

		n, err = conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				go server.closeConnection(conn, ccrEOF, true)
				break ConnLoop
			}

			if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
				go server.closeConnection(conn, ccrTimeout, true)
				break ConnLoop
			}

			go server.closeConnection(conn, ccrUnknown, true)
			if !server.telnetServerConfiguration.SilenceLogs && logh.WarnEnabled {
				server.logger.Warn().Err(err).Msg("telnet connection read: unexpected error")
			}
			break ConnLoop
		}

		if n == 0 {
			continue
		}

		data = append(data, buffer[0:n]...)

		if data[len(data)-1] == lineSeparator {
			byteLines := bytes.Split(data, lineSplitter)
			go func() {
				for _, byteLine := range byteLines {
					if ok := server.telnetHandler.Handle(string(byteLine), ip); ok {
						server.statsTelnetCommandSuccessesInc()
					} else {
						server.statsTelnetCommandFailuresInc()
					}

					server.statsTelnetCommandCountInc()
				}
			}()
			data = make([]byte, 0)
		}
	}

	server.statsNetworkConnectionOpenTime(cFuncListen, startTime)

	if !server.telnetServerConfiguration.SilenceLogs {
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

const (
	cFuncCloseConnection  string = "closeConnection"
	cMsgfErrorClosingConn string = "error closing tcp telnet connection %s (%s): %s"
	cMsgfConnectionClosed string = "tcp telnet connection closed %s (%s) from %d connections)"
	cMsgfClosedConnsStats string = "total telnet connections: %d / %d (local conns / total conns -> %s)"
)

// closeConnection - closes an tcp connection
func (server *Server) closeConnection(conn net.Conn, reason connCloseReason, subtractCounter bool) {

	startTime := time.Now()

	remoteAddressIP := server.extractIP(conn)

	err := conn.Close()
	if err != nil && !server.telnetServerConfiguration.SilenceLogs && logh.ErrorEnabled {
		server.logger.Error().Str(constants.StringsFunc, cFuncCloseConnection).Err(err).Msgf(cMsgfErrorClosingConn, remoteAddressIP, server.telnetHandler.GetSourceType().Name)
	}

	conn = nil

	if reason != ccrMultiple {
		server.connectedIPMap.Delete(remoteAddressIP)
	}

	server.statsNetworkConnectionClose(cFuncCloseConnection, reason)

	var localConns, sharedConns uint32

	if subtractCounter {
		localConns = server.decreaseCounter(&server.numLocalConnections)
		sharedConns = server.decreaseCounter(server.sharedConnectionCounter)
	} else {
		localConns = server.numLocalConnections
		sharedConns = *server.sharedConnectionCounter
	}

	if !server.telnetServerConfiguration.SilenceLogs && logh.InfoEnabled {

		server.logger.Info().Str(constants.StringsFunc, cFuncCloseConnection).Msgf(cMsgfConnectionClosed, remoteAddressIP, reason, localConns)
		server.logger.Info().Str(constants.StringsFunc, cFuncCloseConnection).Msgf(cMsgfClosedConnsStats, localConns, sharedConns, server.telnetHandler.GetSourceType().Name)
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

	<-time.After(server.globalTelnetConfiguration.SendStatsTimeout.Duration)

	err := server.statsTelnetAccumulationHashes()
	if err != nil {
		if logh.ErrorEnabled {
			server.logger.Error().Err(err).Msg("error configuring accumulation hashes")
		}
	}

	for {
		if server.terminate {
			if logh.InfoEnabled {
				server.logger.Info().Str(constants.StringsFunc, cFuncCollectStats).Msg("terminating telnet server stats")
			}
			return
		}

		server.statsNetworkConnection(cFuncCollectStats)

		<-time.After(server.globalTelnetConfiguration.SendStatsTimeout.Duration)
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
