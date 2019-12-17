package udp

import (
	"net"
	"strconv"

	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tsstats"

	"github.com/uol/mycenae/lib/structs"
)

type udpHandler interface {
	HandleUDPpacket(buf []byte, addr string)
	Stop()
}

func New(setUDP structs.SettingsUDP, handler udpHandler, stats *tsstats.StatsTS) *UDPserver {

	return &UDPserver{
		handler:  handler,
		settings: setUDP,
		stats:    stats,
		logger:   logh.CreateContextualLogger(constants.StringsPKG, "udp", "source", "udp-json"),
	}
}

type UDPserver struct {
	handler   udpHandler
	settings  structs.SettingsUDP
	shutdown  bool
	closed    chan struct{}
	stats     *tsstats.StatsTS
	statsTags map[string]string
	logger    *logh.ContextualLogger
}

func (us *UDPserver) Start() {
	go us.asyncStart()
}

const cFuncAsyncStart string = "asyncStart"

func (us *UDPserver) asyncStart() {

	port := ":" + strconv.Itoa(us.settings.Port)

	addr, err := net.ResolveUDPAddr("udp", port)

	if err != nil {
		if logh.FatalEnabled {
			us.logger.Fatal().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Send()
		}
	} else {
		if logh.InfoEnabled {
			us.logger.Info().Str(constants.StringsFunc, cFuncAsyncStart).Msg("addr: resolved")
		}
	}
	sock, err := net.ListenUDP("udp", addr)

	if err != nil {
		if logh.FatalEnabled {
			us.logger.Fatal().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Send()
		}
	} else {
		if logh.InfoEnabled {
			us.logger.Info().Str(constants.StringsFunc, cFuncAsyncStart).Msgf("listen: binded to port: %d", us.settings.Port)
		}
	}
	defer sock.Close()

	err = sock.SetReadBuffer(us.settings.ReadBuffer)

	if err != nil {
		if logh.FatalEnabled {
			us.logger.Fatal().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Send()
		}
	} else {
		if logh.InfoEnabled {
			us.logger.Info().Str(constants.StringsFunc, cFuncAsyncStart).Msg("set buffer: setted")
		}
	}

	for {
		buf := make([]byte, 1024)

		rlen, addr, err := sock.ReadFromUDP(buf)
		us.incConnectionStats()

		saddr := constants.StringsEmpty

		if addr != nil {
			saddr = addr.IP.String()
		}
		if err != nil {
			if logh.ErrorEnabled {
				us.logger.Error().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Msgf("read buffer from %s", saddr)
			}
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
