package telnetsrv

import (
	"bytes"
	"fmt"
	"io"
	"net"
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
		statsConnectionTags: map[string]string{
			"type":   "tcp",
			"port":   strPort,
			"source": telnetHandler.SourceName(),
		},
	}, nil
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

	go func() {

		for {

			conn, err := server.listener.Accept()
			if err != nil {
				server.logger.Error(err.Error(), lf...)
				<-time.After(server.onErrorTimeout)
				continue
			}

			if atomic.LoadUint32(&server.denyNewConnections) == 1 {

				server.logger.Info(fmt.Sprintf("telnet server is not accepting new connections, denying connection from %q", conn.RemoteAddr()), lf...)
				go server.closeConnection(conn, "deny", false)

				continue
			}

			if atomic.LoadUint32(server.sharedConnectionCounter) >= server.maxConnections {

				server.logger.Info(fmt.Sprintf("max number of telnet connections reached (%d), closing connection from %q", server.maxConnections, conn.RemoteAddr()), lf...)
				go server.closeConnection(conn, "limit", false)

				continue
			}

			atomic.AddUint32(server.sharedConnectionCounter, 1)
			atomic.AddUint32(&server.numLocalConnections, 1)

			server.logger.Debug(fmt.Sprintf("received new connection from %q", conn.RemoteAddr()), lf...)

			go server.handleConnection(conn)
		}
	}()

	return nil
}

// handleConnection - handles an incoming connection
func (server *Server) handleConnection(conn net.Conn) {

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "handleConnection"),
	}

	defer server.recover(conn, lf)

	buffer := make([]byte, server.maxBufferSize)
	data := make([]byte, 0)
	var err error
	var n int
	for {
		select {
		case <-(*server.closeConnectionChannel):
			go server.closeConnection(conn, "balancing", true)
			return
		default:
		}

		conn.SetDeadline(time.Now().Add(server.maxIdleConnectionTimeout))

		n, err = conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				go server.closeConnection(conn, "eof", true)
				break
			}

			if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
				go server.closeConnection(conn, "timeout", true)
				break
			}

			go server.closeConnection(conn, "error", true)
			break
		}

		size, err := conn.Write([]byte("OK"))
		if err != nil || size == 0 {
			go server.closeConnection(conn, "write", true)
			break
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

	if err != nil {
		server.logger.Debug(fmt.Sprintf("connection loop was broken under error: %s", err), lf...)
	} else {
		server.logger.Debug("connection loop was broken with no error", lf...)
	}
}

// closeConnection - closes an tcp connection
func (server *Server) closeConnection(conn net.Conn, from string, subtractCounter bool) {

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "closeConnection"),
	}

	server.logger.Info(fmt.Sprintf("closing tcp telnet connection (%s)", from), lf...)

	if subtractCounter {
		atomic.AddUint32(server.sharedConnectionCounter, ^uint32(0))
		atomic.AddUint32(&server.numLocalConnections, ^uint32(0))
	}

	statsCloseTags := map[string]string{
		"type":   from,
		"source": server.telnetHandler.SourceName(),
		"port":   server.port,
	}

	go server.stats.Increment("telnetsrv", "network.connection.close", statsCloseTags)

	if conn != nil {
		err := conn.Close()
		if err != nil {
			server.logger.Error(fmt.Sprintf("error closing tcp telnet connection (%s): %s", from, err.Error()), lf...)
		}

		conn = nil
	}
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

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "collectStats"),
	}

	server.logger.Info("starting telnet server stats", lf...)

	for {
		<-time.After(server.sendStatsTimeout)

		if server.terminate {
			server.logger.Info("terminating telnet server stats", lf...)
			return
		}

		server.stats.ValueAdd("telnetsrv", "network.connection", server.statsConnectionTags, (float64)(server.numLocalConnections))
	}
}

// recover - recovers from panic
func (server *Server) recover(conn net.Conn, lf []zapcore.Field) {

	if r := recover(); r != nil {
		server.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
		atomic.AddUint32(&server.numLocalConnections, ^uint32(0))
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
