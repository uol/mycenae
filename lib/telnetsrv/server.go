package telnetsrv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const lineSeparator byte = 10

// Server - the telnet server struct
type Server struct {
	listenAddress  string
	listener       net.Listener
	onErrorTimeout int
	maxBufferSize  int64
	collector      *collector.Collector
	logger         *zap.Logger
	stats          *tsstats.StatsTS
	telnetHandler  TelnetDataHandler
	lineSplitter   []byte
}

// New - creates a new telnet server
func New(listenAddress string, onErrorTimeout int, maxBufferSize int64, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger, telnetHandler TelnetDataHandler) *Server {

	return &Server{
		listenAddress:  listenAddress,
		onErrorTimeout: onErrorTimeout,
		maxBufferSize:  maxBufferSize,
		collector:      collector,
		logger:         logger,
		stats:          stats,
		telnetHandler:  telnetHandler,
		lineSplitter:   []byte{lineSeparator},
	}
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
				time.Sleep(time.Duration(server.onErrorTimeout) * time.Millisecond)
			}

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

	buffer := make([]byte, server.maxBufferSize)
	data := make([]byte, 0)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			conn.Close()
			if err != io.EOF {
				server.logger.Error(err.Error(), lf...)
			}
			server.logger.Debug("closing tcp telnet connection", lf...)
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

	err := server.listener.Close()
	if err != nil {
		server.logger.Error(err.Error(), lf...)
		return err
	}

	return nil
}
