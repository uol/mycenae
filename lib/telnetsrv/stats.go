package telnetsrv

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
)

//
// Telnet server statistics.
// author: rnojiri
//

const (
	metricNetworkConnectionOpen      string = "network.connection.open"
	metricNetworkConnectionOpenTime  string = "network.connection.open.time"
	metricNetworkConnectionClose     string = "network.connection.close"
	metricNetworkConnectionCloseTime string = "network.connection.close.time"
	stringPort                       string = "port"
	metricTelnetCommandCount         string = "telnet.command.count"
	metricTelnetCommandFailures      string = "telnet.command.fail.count"
	metricTelnetCommandSuccesses     string = "telnet.command.success.count"
)

func (server *Server) statsNetworkConnection(function string) {

	server.timelineManager.FlattenCountN(
		function,
		(float64)(server.numLocalConnections),
		constants.StringsMetricNetworkConnection,
		server.statsConnectionTags...,
	)
}

func (server *Server) statsNetworkIP(function, ip string) {

	server.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricNetworkIP,
		constants.StringsIP, ip,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)
}

func (server *Server) statsNetworkConnectionOpen(function string) {

	server.timelineManager.FlattenCountIncN(
		function,
		metricNetworkConnectionOpen,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)
}

func (server *Server) statsNetworkConnectionOpenTime(function string, startTime time.Time) {

	server.timelineManager.FlattenMaxN(
		function,
		float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond),
		metricNetworkConnectionOpenTime,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)
}

func (server *Server) statsNetworkConnectionClose(function string, reason connCloseReason) {

	server.timelineManager.FlattenCountIncN(
		function,
		metricNetworkConnectionClose,
		stringPort, server.port,
		constants.StringsType, reason,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)
}

func (server *Server) statsNetworkConnectionCloseTime(function string, reason connCloseReason, startTime time.Time) {

	server.timelineManager.FlattenMaxN(
		function,
		float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond),
		metricNetworkConnectionCloseTime,
		stringPort, server.port,
		constants.StringsType, reason,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)
}

func (server *Server) statsTelnetAccumulationHashes() error {

	server.hashMetricTelnetCommandCount = metricTelnetCommandCount + server.telnetHandler.GetSourceType().Name

	err := server.timelineManager.StoreNoTTLCustomHashN(
		server.hashMetricTelnetCommandCount,
		metricTelnetCommandCount,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)

	if err != nil {
		return err
	}

	server.hashMetricTelnetCommandFailures = metricTelnetCommandFailures + server.telnetHandler.GetSourceType().Name

	err = server.timelineManager.StoreNoTTLCustomHashN(
		server.hashMetricTelnetCommandFailures,
		metricTelnetCommandFailures,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)

	if err != nil {
		return err
	}

	server.hashMetricTelnetCommandSuccesses = metricTelnetCommandSuccesses + server.telnetHandler.GetSourceType().Name

	err = server.timelineManager.StoreNoTTLCustomHashN(
		server.hashMetricTelnetCommandSuccesses,
		metricTelnetCommandSuccesses,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.GetSourceType().Name,
	)

	if err != nil {
		return err
	}

	server.logger.Info().Msg("telnet accumulation hashes stored")

	return nil
}

func (server *Server) statsTelnetCommandCountInc() {

	server.timelineManager.AccumulateCustomHashN(server.hashMetricTelnetCommandCount)
}

func (server *Server) statsTelnetCommandFailuresInc() {

	server.timelineManager.AccumulateCustomHashN(server.hashMetricTelnetCommandFailures)
}

func (server *Server) statsTelnetCommandSuccessesInc() {

	server.timelineManager.AccumulateCustomHashN(server.hashMetricTelnetCommandSuccesses)
}
