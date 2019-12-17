package collector

import (
	"github.com/uol/gobol"
	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"
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

	defer func() {
		if r := recover(); r != nil {
			if logh.ErrorEnabled {
				collector.logger.Error().Str(constants.StringsFunc, "fail").Str("addr", addr).Msgf("panic recovery: %v", r)
			}
		}
	}()

	if logh.ErrorEnabled {
		collector.logger.Error().Str(constants.StringsFunc, "fail").Str("addr", addr).Err(gerr)
	}
}
