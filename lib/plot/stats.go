package plot

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
)

func (plot *Plot) statsQueryTSThreshold(ksid string, total int) {
	go plot.statsValueMax(
		"mycenae.query.threshold",
		map[string]string{"keyset": ksid},
		float64(total),
	)
}

func (plot *Plot) statsQueryTSLimit(ksid string, total int) {
	go plot.statsValueMax(
		"mycenae.query.limit",
		map[string]string{"keyset": ksid},
		float64(total),
	)
}

func (plot *Plot) statsSelectQerror(ks, cf string) {
	go plot.statsIncrement(
		"scylla.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func (plot *Plot) statsSelectFerror(ks, cf string) {
	go plot.statsIncrement(
		"scylla.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func (plot *Plot) statsIndexError(i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != constants.StringsEmpty {
		tags["type"] = t
	}
	go plot.statsIncrement("solr.request.error", tags)
}

func (plot *Plot) statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
	if t != constants.StringsEmpty {
		tags["type"] = t
	}
	go plot.statsIncrement("solr.request", tags)
	go plot.statsValueAdd(
		"solr.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func (plot *Plot) statsSelect(ks, cf string, d time.Duration, countRows int) {
	tags := map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"}
	go plot.statsIncrement("scylla.query", tags)
	go plot.statsValueAdd(
		"scylla.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
	go plot.statsValueMax("scylla.query.max.rows", tags, float64(countRows))
}

func (plot *Plot) statsPlotSummaryPoints(count, total int, bytes uint32, keyset string) {
	go plot.statsValueMax("plot.count.points", map[string]string{"keyset": keyset}, float64(count))
	go plot.statsValueMax("plot.total.points", map[string]string{"keyset": keyset}, float64(total))
	go plot.statsValueMax("plot.bytes.points", map[string]string{"keyset": keyset}, float64(bytes))
}

func (plot *Plot) statsConferMetric(keyset, metric string) {
	go plot.statsAnalyticIncrement("good.metric", map[string]string{"keyset": keyset, "metric": metric})
}

func (plot *Plot) statsIncrement(metric string, tags map[string]string) {
	plot.stats.Increment(cPackage, metric, tags)
}

func (plot *Plot) statsValueAdd(metric string, tags map[string]string, v float64) {
	plot.stats.ValueAdd(cPackage, metric, tags, v)
}

func (plot *Plot) statsValueMax(metric string, tags map[string]string, v float64) {
	plot.stats.ValueMax(cPackage, metric, tags, v)
}

func (plot *Plot) statsAnalyticIncrement(metric string, tags map[string]string) {
	plot.stats.AnalyticIncrement(cPackage, metric, tags)
}
