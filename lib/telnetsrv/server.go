package telnetsrv

import (
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Server - the telnet server struct
type Server struct {
	listenAddress  string
	listener       net.Listener
	onErrorTimeout int
	maxBufferSize  int
	collector      *collector.Collector
	logger         *zap.Logger
	formatRegexp   *regexp.Regexp
	tagsRegexp     *regexp.Regexp
	stats          *tsstats.StatsTS
}

// New - creates a new telnet server
func New(listenAddress string, onErrorTimeout, maxBufferSize int, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger) *Server {

	return &Server{
		listenAddress:  listenAddress,
		onErrorTimeout: onErrorTimeout,
		maxBufferSize:  maxBufferSize,
		collector:      collector,
		logger:         logger,
		formatRegexp:   regexp.MustCompile(`^put ([0-9A-Za-z-\._\%\&\#\;\/]+) ([0-9]+) ([0-9E\.\-\,]+) ([0-9A-Za-z-\._\%\&\#\;\/ =]+)$`),
		tagsRegexp:     regexp.MustCompile(`([0-9A-Za-z-\._\%\&\#\;\/]+)=([0-9A-Za-z-\._\%\&\#\;\/]+)`),
		stats:          stats,
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
		zap.String("func", "ListenAndServe"),
	}

	server.logger.Info(fmt.Sprintf("listening telnet connections at %q...", server.listener.Addr()), lf...)

	go func() {

		for {

			conn, err := server.listener.Accept()
			if err != nil {
				server.logger.Error(err.Error(), lf...)
				time.Sleep(time.Duration(server.onErrorTimeout) * time.Millisecond)
			}

			server.logger.Debug(fmt.Sprintf("received new connection from %q", conn.RemoteAddr()), lf...)

			go server.handleConnection(conn)
		}
	}()

	return nil
}

// handleConnection - handles an incoming connection
func (server *Server) handleConnection(conn net.Conn) {

	defer conn.Close()

	lf := []zapcore.Field{
		zap.String("package", "telnetsrv"),
		zap.String("func", "handleConnection"),
	}

	// Make a buffer to hold incoming data.
	buffer := make([]byte, server.maxBufferSize)

	// Read the incoming connection into the buffer.
	numRead, err := conn.Read(buffer)
	if err != nil {
		server.logger.Error("error reading: "+err.Error(), lf...)
		return
	}

	if numRead == 0 {
		server.logger.Warn("received an empty message", lf...)
		return
	}

	data := string(buffer[0:numRead])

	server.logger.Debug("received: "+data, lf...)

	server.handlePoints(&data)

	conn.Write([]byte("OK"))
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
