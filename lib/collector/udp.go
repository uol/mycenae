package collector

import (
	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
)

// HandleUDPpacket - handles the UDP packet received from the collector
func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {

	statsNetworkIP(addr, constants.StringsUDP)

	if logh.DebugEnabled {
		logh.Debug().Msgf("udp: %s", string(buf))
	}

	_, gerr := collector.HandleJSONBytes(buf, constants.StringsUDP, true)
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
