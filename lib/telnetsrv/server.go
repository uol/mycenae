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
	listenAddress        string
	listener             net.Listener
	onErrorTimeout       time.Duration
	sendStatsTimeout     time.Duration
	maxBufferSize        int64
	collector            *collector.Collector
	logger               *zap.Logger
	stats                *tsstats.StatsTS
	telnetHandler        TelnetDataHandler
	lineSplitter         []byte
	statsTags            map[string]string
	numConnections       uint32
	terminate            bool
	sendStatsTimeoutFreq string
}

// New - creates a new telnet server
func New(listenAddress, onErrorTimeout, sendStatsTimeout string, maxBufferSize int64, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger, telnetHandler TelnetDataHandler) (*Server, error) {

	onErrorTimeoutDuration, err := time.ParseDuration(onErrorTimeout)
	if err != nil {
		return nil, err
	}

	sendStatsTimeoutDuration, err := time.ParseDuration(sendStatsTimeout)
	if err != nil {
		return nil, err
	}

	return &Server{
		listenAddress:        listenAddress,
		onErrorTimeout:       onErrorTimeoutDuration,
		sendStatsTimeoutFreq: "@every " + sendStatsTimeout,
		sendStatsTimeout:     sendStatsTimeoutDuration,
		maxBufferSize:        maxBufferSize,
		collector:            collector,
		logger:               logger,
		stats:                stats,
		telnetHandler:        telnetHandler,
		lineSplitter:         []byte{lineSeparator},
		terminate:            false,
		statsTags: map[string]string{
			"type":   "tcp",
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

			atomic.AddUint32(&server.numConnections, 1)

			conn.SetDeadline(time.Time{})

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
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				server.logger.Error(err.Error(), lf...)
			}
			err := conn.Close()
			if err != nil {
				server.logger.Error(err.Error(), lf...)
			}
			server.logger.Info("closing tcp telnet connection", lf...)
			atomic.AddUint32(&server.numConnections, ^uint32(0))
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

		server.stats.ValueAdd("telnetsrv", "network.connection", server.statsTags, (float64)(server.numConnections))
	}
}

// recover - recovers from panic
func (server *Server) recover(conn net.Conn, lf []zapcore.Field) {

	if conn != nil {
		err := conn.Close()
		if err != nil {
			server.logger.Error(err.Error(), lf...)
		}
	}

	if r := recover(); r != nil {
		server.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
		atomic.AddUint32(&server.numConnections, ^uint32(0))
	}
}
