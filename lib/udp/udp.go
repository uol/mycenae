package udp

import (
	"fmt"
	"net"

	"github.com/uol/mycenae/lib/structs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	gblog *zap.Logger
)

type udpHandler interface {
	HandleUDPpacket(buf []byte, addr string)
	Stop()
}

func New(gbl *zap.Logger, setUDP structs.SettingsUDP, handler udpHandler) *UDPserver {

	gblog = gbl

	return &UDPserver{
		handler:  handler,
		settings: setUDP,
	}
}

type UDPserver struct {
	handler  udpHandler
	settings structs.SettingsUDP
	shutdown bool
	closed   chan struct{}
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
		gblog.Fatal(fmt.Sprintf("addr: %s", err.Error()), lf...)
	} else {
		gblog.Info("addr: resolved", lf...)
	}
	sock, err := net.ListenUDP("udp", addr)

	if err != nil {
		gblog.Fatal(fmt.Sprintf("listen: %s", err.Error()), lf...)
	} else {
		gblog.Info(fmt.Sprintf("listen: binded to port: %s", us.settings.Port), lf...)
	}
	defer sock.Close()

	err = sock.SetReadBuffer(us.settings.ReadBuffer)

	if err != nil {
		gblog.Fatal(fmt.Sprintf("set buffer: %s", err.Error()), lf...)
	} else {
		gblog.Info("set buffer: setted", lf...)
	}

	for {
		buf := make([]byte, 1024)

		rlen, addr, err := sock.ReadFromUDP(buf)

		saddr := ""

		if addr != nil {
			saddr = addr.IP.String()
		}
		if err != nil {
			gblog.Error(fmt.Sprintf("read buffer from %s : %s", saddr, err), lf...)
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
