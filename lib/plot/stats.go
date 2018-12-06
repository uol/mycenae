package plot

import (
	"time"
)

func statsQueryTSThreshold(ksid, metric string, total int) {
	go statsValueAdd(
		"mycenae.query.threshold",
		map[string]string{"keyset": ksid, "metric": metric},
		float64(total),
	)
}

func statsQueryTSLimit(ksid, metric string, total int) {
	go statsValueAdd(
		"mycenae.query.limit",
		map[string]string{"keyset": ksid, "metric": metric},
		float64(total),
	)
}

func statsSelectQerror(ks, cf string) {
	go statsIncrement(
		"cassandra.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsSelectFerror(ks, cf string) {
	go statsIncrement(
		"cassandra.fallback.error",
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

func statsSelect(ks, cf string, d time.Duration) {
	go statsIncrement("cassandra.query", map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"})
	go statsValueAdd(
		"cassandra.query.duration",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("plot/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("plot/persistence", metric, tags, v)
}
