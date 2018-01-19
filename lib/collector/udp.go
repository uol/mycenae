package collector

import (
	"encoding/json"

	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
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
	defer func() {
		if r := recover(); r != nil {
			gblog.WithFields(
				logrus.Fields{
					"func":    "fail",
					"package": "Collector",
				},
			).Errorf("Panic: %v", r)
		}
	}()

	fields := gerr.LogFields()
	fields["addr"] = addr

	gblog.WithFields(fields).Error(gerr.Error())
}
