package collector

import (
	"fmt"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func sendIPStats(addr string) {

	go stats.Increment("HandleUDPpacket", "network.ip", map[string]string{"ip": addr, "source": "udp"})
}

// HandleUDPpacket - handles the UDP packet received from the collector
func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {

	sendIPStats(addr)

	_, gerr := collector.HandleJSONBytes(buf, "udp", true)
	if gerr != nil {
		collector.fail(gerr, addr)
	}
}

func (collector *Collector) fail(gerr gobol.Error, addr string) {

	lf := []zapcore.Field{
		zap.String("package", "Collector"),
		zap.String("func", "fail"),
		zap.String("addr", addr),
	}

	defer func() {
		if r := recover(); r != nil {
			gblog.Error(fmt.Sprintf("Panic: %v", r), lf...)
		}
	}()

	gblog.Debug(gerr.Error(), lf...)
}
