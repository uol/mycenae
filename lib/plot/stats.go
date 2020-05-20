package plot

import (
	"github.com/uol/mycenae/lib/constants"
)

const (
	metricQueryThreshold  string = "mycenae.query.threshold"
	metricQueryLimit      string = "mycenae.query.limit"
	metricPlotCountPoints string = "plot.count.points"
	metricPlotTotalPoints string = "plot.total.points"
	metricActiveMetric    string = "mycenae.active.metric"
)

func (plot *Plot) statsQueryTSThreshold(function, keyset string, total int) {

	plot.timelineManager.FlattenMaxN(
		function,
		float64(total),
		metricQueryThreshold,
		constants.StringsKeyset, keyset,
	)
}

func (plot *Plot) statsQueryTSLimit(function, keyset string, total int) {

	plot.timelineManager.FlattenMaxN(
		function,
		float64(total),
		metricQueryLimit,
		constants.StringsKeyset, keyset,
	)
}

func (plot *Plot) statsPlotSummaryPoints(function, keyset string, count, total int) {

	plot.timelineManager.FlattenMaxN(
		function,
		float64(count),
		metricPlotCountPoints,
		constants.StringsKeyset, keyset,
	)

	plot.timelineManager.FlattenMaxN(
		function,
		float64(total),
		metricPlotTotalPoints,
		constants.StringsKeyset, keyset,
	)
}

func (plot *Plot) statsActiveMetric(function, keyset, metric string) {

	plot.timelineManager.FlattenCountIncA(
		function,
		metricActiveMetric,
		constants.StringsKeyset, keyset,
		constants.StringsMetric, metric,
	)
}
