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

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//
// Implements a telnet server to input data
// author: rnojiri
//

const lineSeparator byte = 10

var (
	closeConnectionLogFields = []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "closeConnection"),
	}

	collectStatsLogFields = []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "collectStats"),
	}

	handleConnectionLogFields = []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "handleConnection"),
	}
)

// Server - the telnet server struct
type Server struct {
	listenAddress            string
	listener                 net.Listener
	onErrorTimeout           time.Duration
	sendStatsTimeout         time.Duration
	maxIdleConnectionTimeout time.Duration
	maxBufferSize            int64
	collector                *collector.Collector
	logger                   *zap.Logger
	stats                    *tsstats.StatsTS
	telnetHandler            TelnetDataHandler
	lineSplitter             []byte
	statsConnectionTags      map[string]string
	sharedConnectionCounter  *uint32
	numLocalConnections      uint32
	maxConnections           uint32
	denyNewConnections       uint32
	terminate                bool
	port                     string
	name                     string
	closeConnectionChannel   *chan struct{}
	connectedIPMap           sync.Map
}

// New - creates a new telnet server
func New(serverConfiguration *structs.TelnetServerConfiguration, sharedConnectionCounter *uint32, maxConnections uint32, closeConnectionChannel *chan struct{}, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger, telnetHandler TelnetDataHandler) (*Server, error) {

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
		logger:                   logger,
		stats:                    stats,
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
		statsConnectionTags: map[string]string{
			"type":   "tcp",
			"port":   strPort,
			"source": telnetHandler.SourceName(),
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

// Listen - starts to listen and to handle the incoming messages
func (server *Server) Listen() error {

	var err error
	server.listener, err = net.Listen("tcp", server.listenAddress)
	if nil != err {
		return err
	}

	go server.collectStats()

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "Listen"),
	}

	server.logger.Info(fmt.Sprintf("listening telnet connections at %q...", server.listener.Addr()), lf...)

	handlerSourceName := server.telnetHandler.SourceName()

	go func() {

		for {

			conn, err := server.listener.Accept()
			if err != nil {
				server.logger.Error(err.Error(), lf...)
				<-time.After(server.onErrorTimeout)
				continue
			}

			remoteAddressIP := server.extractIP(conn)

			// reports the connection IP and source
			go server.stats.ValueAdd("telnetsrv", "network.ip", map[string]string{"ip": remoteAddressIP, "source": handlerSourceName}, 1)
			go server.stats.ValueAdd("telnetsrv", "network.connection.open", map[string]string{"source": handlerSourceName, "port": server.port}, 1)

			if _, stored := server.connectedIPMap.LoadOrStore(remoteAddressIP, struct{}{}); stored {

				server.logger.Info(fmt.Sprintf("telnet server will not accept new connections from %s", remoteAddressIP), lf...)
				go server.closeConnection(conn, "multiple", false)

				continue
			}

			if atomic.LoadUint32(&server.denyNewConnections) == 1 {

				server.logger.Info(fmt.Sprintf("telnet server is not accepting new connections, denying connection from %s", remoteAddressIP), lf...)
				go server.closeConnection(conn, "deny", false)

				continue
			}

			if atomic.LoadUint32(server.sharedConnectionCounter) >= server.maxConnections {

				server.logger.Info(fmt.Sprintf("max number of telnet connections reached (%d), closing connection from %s", server.maxConnections, remoteAddressIP), lf...)
				go server.closeConnection(conn, "limit", false)

				continue
			}

			server.increaseCounter(server.sharedConnectionCounter)
			server.increaseCounter(&server.numLocalConnections)

			server.logger.Info(fmt.Sprintf("received new connection from %s", remoteAddressIP), lf...)

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

	defer server.recover(conn, handleConnectionLogFields)

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
			for _, byteLine := range byteLines {
				server.telnetHandler.Handle(string(byteLine))
			}
			data = make([]byte, 0)
		}
	}

	go server.stats.ValueAdd(
		"telnetsrv",
		"network.connection.open.time",
		map[string]string{
			"source": server.telnetHandler.SourceName(),
			"port":   server.port,
		},
		float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond),
	)

	if err != nil {
		server.logger.Error(fmt.Sprintf("connection loop was broken under error: %s", err), handleConnectionLogFields...)
	} else {
		server.logger.Debug("connection loop was broken with no error", handleConnectionLogFields...)
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

// closeConnection - closes an tcp connection
func (server *Server) closeConnection(conn net.Conn, reason string, subtractCounter bool) {

	startTime := time.Now()

	remoteAddressIP := server.extractIP(conn)

	err := conn.Close()
	if err != nil {
		server.logger.Error(fmt.Sprintf("error closing tcp telnet connection %s (%s): %s", remoteAddressIP, reason, err.Error()), closeConnectionLogFields...)
	}

	conn = nil

	server.connectedIPMap.Delete(remoteAddressIP)

	source := server.telnetHandler.SourceName()

	statsCloseTags := map[string]string{
		"type":   reason,
		"source": source,
		"port":   server.port,
	}

	go server.stats.Increment("telnetsrv", "network.connection.close", statsCloseTags)

	var localConns, sharedConns uint32

	if subtractCounter {
		localConns = server.decreaseCounter(&server.numLocalConnections)
		sharedConns = server.decreaseCounter(server.sharedConnectionCounter)
	} else {
		localConns = server.numLocalConnections
		sharedConns = *server.sharedConnectionCounter
	}

	server.logger.Info(fmt.Sprintf("tcp telnet connection closed %s (%s) from %d connections)", remoteAddressIP, reason, localConns), closeConnectionLogFields...)

	server.logger.Info(fmt.Sprintf("total telnet connections: %d / %d (local conns / total conns -> %s)", localConns, sharedConns, source), closeConnectionLogFields...)

	go server.stats.ValueAdd("telnetsrv", "network.connection.close.time", statsCloseTags, float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond))
}

// Shutdown - stops listening
func (server *Server) Shutdown() error {

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "Shutdown"),
	}

	server.terminate = true

	err := server.listener.Close()
	if err != nil {
		server.logger.Error(err.Error(), lf...)
		return err
	}

	server.logger.Info("telnet server have shut down", lf...)

	return nil
}

// collectStats - send all TCP connection statistics
func (server *Server) collectStats() {

	server.logger.Info("starting telnet server stats", collectStatsLogFields...)

	for {
		<-time.After(server.sendStatsTimeout)

		if server.terminate {
			server.logger.Info("terminating telnet server stats", collectStatsLogFields...)
			return
		}

		go server.stats.ValueAdd("telnetsrv", "network.connection", server.statsConnectionTags, (float64)(server.numLocalConnections))
	}
}

// recover - recovers from panic
func (server *Server) recover(conn net.Conn, lf []zapcore.Field) {

	if r := recover(); r != nil {
		server.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
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
