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
		constants.StringsSource, server.telnetHandler.SourceName(),
	)
}

func (server *Server) statsNetworkConnectionOpen(function string) {

	server.timelineManager.FlattenCountIncN(
		function,
		metricNetworkConnectionOpen,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.SourceName(),
	)
}

func (server *Server) statsNetworkConnectionOpenTime(function string, startTime time.Time) {

	server.timelineManager.FlattenMaxN(
		function,
		float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond),
		metricNetworkConnectionOpenTime,
		stringPort, server.port,
		constants.StringsSource, server.telnetHandler.SourceName(),
	)
}

func (server *Server) statsNetworkConnectionClose(function, reason string) {

	server.timelineManager.FlattenCountIncN(
		function,
		metricNetworkConnectionClose,
		stringPort, server.port,
		constants.StringsType, reason,
		constants.StringsSource, server.telnetHandler.SourceName(),
	)
}

func (server *Server) statsNetworkConnectionCloseTime(function, reason string, startTime time.Time) {

	server.timelineManager.FlattenMaxN(
		function,
		float64(time.Since(startTime).Nanoseconds())/float64(time.Millisecond),
		metricNetworkConnectionCloseTime,
		stringPort, server.port,
		constants.StringsType, reason,
		constants.StringsSource, server.telnetHandler.SourceName(),
	)
}
