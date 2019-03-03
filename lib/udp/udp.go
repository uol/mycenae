package udp

import (
	"fmt"
	"net"

	"github.com/uol/mycenae/lib/tsstats"

	"github.com/uol/mycenae/lib/structs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type udpHandler interface {
	HandleUDPpacket(buf []byte, addr string)
	Stop()
}

func New(logger *zap.Logger, setUDP structs.SettingsUDP, handler udpHandler, stats *tsstats.StatsTS) *UDPserver {

	return &UDPserver{
		handler:  handler,
		settings: setUDP,
		stats:    stats,
		logger:   logger,
		statsTags: map[string]string{
			"type":   "udp",
			"source": "udp-json",
		},
	}
}

type UDPserver struct {
	handler   udpHandler
	settings  structs.SettingsUDP
	shutdown  bool
	closed    chan struct{}
	stats     *tsstats.StatsTS
	statsTags map[string]string
	logger    *zap.Logger
}

func (us UDPserver) Start() {
	go us.asyncStart()
}

func (us UDPserver) asyncStart() {

	lf := []zapcore.Field{
		zap.String("package", "udp"),
		zap.String("func", "asyncStart"),
	}

	port := ":" + us.settings.Port

	addr, err := net.ResolveUDPAddr("udp", port)

	if err != nil {
		us.logger.Fatal(fmt.Sprintf("addr: %s", err.Error()), lf...)
	} else {
		us.logger.Info("addr: resolved", lf...)
	}
	sock, err := net.ListenUDP("udp", addr)

	if err != nil {
		us.logger.Fatal(fmt.Sprintf("listen: %s", err.Error()), lf...)
	} else {
		us.logger.Info(fmt.Sprintf("listen: binded to port: %s", us.settings.Port), lf...)
	}
	defer sock.Close()

	err = sock.SetReadBuffer(us.settings.ReadBuffer)

	if err != nil {
		us.logger.Fatal(fmt.Sprintf("set buffer: %s", err.Error()), lf...)
	} else {
		us.logger.Info("set buffer: setted", lf...)
	}

	for {
		buf := make([]byte, 1024)

		rlen, addr, err := sock.ReadFromUDP(buf)
		us.incConnectionStats()

		saddr := ""

		if addr != nil {
			saddr = addr.IP.String()
		}
		if err != nil {
			us.logger.Error(fmt.Sprintf("read buffer from %s : %s", saddr, err), lf...)
		} else {
			go us.handler.HandleUDPpacket(buf[0:rlen], saddr)
		}

		if us.shutdown {
			us.closed <- struct{}{}
			return
		}
	}
}

func (us *UDPserver) Stop() {
	us.shutdown = true
	select {
	case <-us.closed:
		us.handler.Stop()
		return
	}
}

// incConnectionStats - increments the UDP connection statistics
func (us *UDPserver) incConnectionStats() {
	go us.stats.Increment("udp", "network.connection", us.statsTags)
}
