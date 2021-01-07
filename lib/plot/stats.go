package plot

import (
	"github.com/uol/mycenae/lib/constants"
)

const (
	metricQueryThreshold    string = "mycenae.query.threshold"
	metricQueryLimit        string = "mycenae.query.limit"
	metricPlotCountPoints   string = "plot.count.points"
	metricPlotTotalPoints   string = "plot.total.points"
	metricActiveMetric      string = "mycenae.active.metric"
	metricDeleteMetaError   string = "metadata.delete.error"
	metricDeleteMetaSuccess string = "metadata.delete.success"
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

func (plot *Plot) statsDeleteMetaError(function, keyset, metric string) {
	plot.timelineManager.FlattenCountIncA(
		function,
		metricDeleteMetaError,
		constants.StringsKeyset, keyset,
		constants.StringsMetric, metric,
	)
}

func (plot *Plot) statsDeleteMetaSuccess(function, keyset, metric string) {
	plot.timelineManager.FlattenCountIncA(
		function,
		metricDeleteMetaSuccess,
		constants.StringsKeyset, keyset,
		constants.StringsMetric, metric,
	)
}
