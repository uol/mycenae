package plot

import (
	"time"
)

func statsQueryTSThreshold(ksid, metric string, total int) {
	go statsValueMax(
		"mycenae.query.threshold",
		map[string]string{"keyset": ksid, "metric": metric},
		float64(total),
	)
}

func statsQueryTSLimit(ksid, metric string, total int) {
	go statsValueMax(
		"mycenae.query.limit",
		map[string]string{"keyset": ksid, "metric": metric},
		float64(total),
	)
}

func statsSelectQerror(ks, cf string) {
	go statsIncrement(
		"scylla.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsSelectFerror(ks, cf string) {
	go statsIncrement(
		"scylla.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsIndexError(i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("solr.request.error", tags)
}

func statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("solr.request", tags)
	go statsValueAdd(
		"solr.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsSelect(ks, cf string, d time.Duration, countRows int) {
	tags := map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"}
	go statsIncrement("scylla.query", tags)
	go statsValueAdd(
		"scylla.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
	go statsValueMax("scylla.query.max.rows", tags, float64(countRows))
}

func statsPlotSummaryPoints(count, total int, keyset string) {
	go statsValueMax("plot.count.points", map[string]string{"keyset": keyset}, float64(count))
	go statsValueMax("plot.total.points", map[string]string{"keyset": keyset}, float64(total))
}

func statsConferMetric(keyset, metric string) {
	go statsAnalyticIncrement("good.metric", map[string]string{"keyset": keyset, "metric": metric})
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("plot", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("plot", metric, tags, v)
}

func statsValueMax(metric string, tags map[string]string, v float64) {
	stats.ValueMax("plot", metric, tags, v)
}

func statsAnalyticIncrement(metric string, tags map[string]string) {
	stats.AnalyticIncrement("plot", metric, tags)
}
