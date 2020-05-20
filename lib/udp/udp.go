package udp

import (
	"net"
	"strconv"

	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/stats"
	"github.com/uol/mycenae/lib/utils"

	"github.com/uol/mycenae/lib/structs"
)

type udpHandler interface {
	HandleUDPpacket(buf []byte, addr string)
	Stop()
}

// New - creates a new udp server instance
func New(setUDP structs.SettingsUDP, handler udpHandler, timelineManager *stats.TimelineManager) *UDPserver {

	return &UDPserver{
		handler:         handler,
		settings:        setUDP,
		timelineManager: timelineManager,
		logger:          logh.CreateContextualLogger(constants.StringsPKG, "udp", "source", "udp-json"),
	}
}

// UDPserver - the server struct
type UDPserver struct {
	handler         udpHandler
	settings        structs.SettingsUDP
	sock            *net.UDPConn
	timelineManager *stats.TimelineManager
	logger          *logh.ContextualLogger
}

// Start - starts the udp server
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
	us.sock, err = net.ListenUDP("udp", addr)

	if err != nil {
		if logh.FatalEnabled {
			us.logger.Fatal().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Send()
		}
	} else {
		if logh.InfoEnabled {
			us.logger.Info().Str(constants.StringsFunc, cFuncAsyncStart).Msgf("listen: binded to port: %d", us.settings.Port)
		}
	}
	defer us.sock.Close()

	err = us.sock.SetReadBuffer(us.settings.ReadBuffer)

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

		rlen, addr, err := us.sock.ReadFromUDP(buf)
		us.statsNetworkConnection(cFuncAsyncStart)

		saddr := constants.StringsEmpty

		if addr != nil {
			saddr = addr.IP.String()
		}
		if err != nil {
			if utils.IsConnectionClosedError(err) {
				break
			}

			if logh.ErrorEnabled {
				us.logger.Error().Str(constants.StringsFunc, cFuncAsyncStart).Err(err).Msgf("read buffer from %s", saddr)
			}
		} else {
			go us.handler.HandleUDPpacket(buf[0:rlen], saddr)
		}
	}

	if logh.InfoEnabled {
		us.logger.Info().Str(constants.StringsFunc, cFuncAsyncStart).Msg("stopping to listen udp packets")
	}
}

// Stop - stops the udp server
func (us *UDPserver) Stop() {
	err := us.sock.Close()
	if err != nil {
		if logh.ErrorEnabled {
			us.logger.Error().Str(constants.StringsFunc, "Stop").Err(err).Send()
		}
	}
}
