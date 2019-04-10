package telnetsrv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
	numConnections           uint32
	maxTelnetConnections     uint32
	sharedConnectionCounter  *uint32
	terminate                bool
	port                     string
}

// New - creates a new telnet server
func New(host string, port int, onErrorTimeout, sendStatsTimeout, maxIdleConnectionTimeout string, maxBufferSize int64, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger, sharedConnectionCounter *uint32, maxTelnetConnections uint32, telnetHandler TelnetDataHandler) (*Server, error) {

	onErrorTimeoutDuration, err := time.ParseDuration(onErrorTimeout)
	if err != nil {
		return nil, err
	}

	sendStatsTimeoutDuration, err := time.ParseDuration(sendStatsTimeout)
	if err != nil {
		return nil, err
	}

	maxIdleConnectionTimeoutDuration, err := time.ParseDuration(maxIdleConnectionTimeout)
	if err != nil {
		return nil, err
	}

	strPort := fmt.Sprintf("%d", port)

	return &Server{
		listenAddress:            fmt.Sprintf("%s:%d", host, port),
		onErrorTimeout:           onErrorTimeoutDuration,
		sendStatsTimeout:         sendStatsTimeoutDuration,
		maxIdleConnectionTimeout: maxIdleConnectionTimeoutDuration,
		maxBufferSize:            maxBufferSize,
		collector:                collector,
		logger:                   logger,
		stats:                    stats,
		telnetHandler:            telnetHandler,
		lineSplitter:             []byte{lineSeparator},
		terminate:                false,
		port:                     strPort,
		sharedConnectionCounter:  sharedConnectionCounter,
		maxTelnetConnections:     maxTelnetConnections,
		statsConnectionTags: map[string]string{
			"type":   "tcp",
			"port":   strPort,
			"source": telnetHandler.SourceName(),
		},
	}, nil
}

// Listen - starts to listen and to handle the incoming messages
func (server *Server) Listen() error {

	if "" == server.listenAddress {
		server.listenAddress = ":telnet"
	}

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

			if atomic.LoadUint32(server.sharedConnectionCounter) >= server.maxTelnetConnections {

				server.logger.Info(fmt.Sprintf("max number of telnet connections reached (%d), closing connection from %q", server.maxTelnetConnections, conn.RemoteAddr()), lf...)

				err = conn.Close()
				if err != nil {
					server.logger.Error(fmt.Sprintf("error closing tcp telnet connection (%s): %s", conn.RemoteAddr(), err.Error()), lf...)
				}

				continue
			}

			atomic.AddUint32(server.sharedConnectionCounter, 1)
			atomic.AddUint32(&server.numConnections, 1)

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
	for {
		conn.SetDeadline(time.Now().Add(server.maxIdleConnectionTimeout))

		n, err := conn.Read(buffer)
		if err != nil && err == io.EOF {
			server.closeConnection(conn, "eof")
			break
		}

		if err, ok := err.(net.Error); ok && err.Timeout() {
			server.closeConnection(conn, "timeout")
			break
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
}

// closeConnection - closes an tcp connection
func (server *Server) closeConnection(conn net.Conn, from string) {

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "closeConnection"),
	}

	server.logger.Info(fmt.Sprintf("closing tcp telnet connection (%s)", from), lf...)
	atomic.AddUint32(server.sharedConnectionCounter, ^uint32(0))
	atomic.AddUint32(&server.numConnections, ^uint32(0))

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

		server.stats.ValueAdd("telnetsrv", "network.connection", server.statsConnectionTags, (float64)(server.numConnections))
	}
}

// recover - recovers from panic
func (server *Server) recover(conn net.Conn, lf []zapcore.Field) {

	if r := recover(); r != nil {
		server.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
		atomic.AddUint32(&server.numConnections, ^uint32(0))
	}
}
