package collector

import (
	"fmt"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {
	go func() {
		collector.saveMutex.Lock()
		collector.saving++
		collector.saveMutex.Unlock()
	}()

	rcvMsg := TSDBpoint{}

	var gerr gobol.Error

	err := json.Unmarshal(buf, &rcvMsg)
	if err != nil {
		gerr = errUnmarshal("HandleUDPpacket", err)
		collector.fail(gerr, addr)
		return
	}

	logFields := map[string]string{}
	logFields["addr"] = addr

	collector.HandlePacket(rcvMsg, nil, true, "udp", logFields)

	go func() {
		collector.saveMutex.Lock()
		collector.saving--
		collector.saveMutex.Unlock()
	}()
}

func (collector *Collector) fail(gerr gobol.Error, addr string) {
	lf := []zapcore.Field{
		zap.String("package", "Collector"),
		zap.String("func", "fail"),
	}

	defer func() {
		if r := recover(); r != nil {
			gblog.Error(fmt.Sprintf("Panic: %v", r), lf...)
		}
	}()

	fields := gerr.LogFields()
	fields = append(fields, zap.String("addr", addr))

	gblog.Error(gerr.Error(), lf...)
}
