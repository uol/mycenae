package collector

import (
	"strconv"
	"time"
)

func statsProcTime(ksid string, d time.Duration) {
	go statsValueAdd(
		"points.processes_time",
		map[string]string{"target_ksid": ksid},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsLostMeta() {
	go statsIncrement(
		"meta.lost",
		map[string]string{},
	)
}

func statsInsertQerror(ksid, cf string) {
	go statsIncrement(
		"cassandra.query.error",
		map[string]string{"target_ksid": ksid, "column_family": cf, "operation": "insert"},
	)
}

func statsInsertFBerror(ksid, cf string) {
	go statsIncrement(
		"cassandra.fallback.error",
		map[string]string{"target_ksid": ksid, "column_family": cf, "operation": "insert"},
	)
}

func statsIndexError(i, t, m string) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("elastic.request.error", tags)
}

func statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("elastic.request", tags)
	go statsValueAdd(
		"elastic.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsBulkPoints() {
	go statsIncrement("elastic.bulk.points", map[string]string{})
}

func statsInsert(ksid, cf string, d time.Duration) {
	go statsIncrement("cassandra.query", map[string]string{"target_ksid": ksid, "column_family": cf, "operation": "insert"})
	go statsValueAdd(
		"cassandra.query.duration",
		map[string]string{"target_ksid": ksid, "column_family": cf, "operation": "insert"},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsPoints(ksid, vt, protocol, ttl string) {
	go statsIncrement(
		"points.received",
		map[string]string{"protocol": protocol, "target_ksid": ksid, "type": vt, "target_ttl": ttl},
	)
}

func statsPointsError(ksid, vt, protocol, ttl string) {
	go statsIncrement(
		"points.received.error",
		map[string]string{"protocol": protocol, "target_ksid": ksid, "type": vt, "target_ttl": ttl},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("collector", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("collector", metric, tags, v)
}

func statsCountNewTimeseries(ksid, vt string, ttl uint8) {
	go statsIncrement(
		"timeseries.count.new",
		map[string]string{"target_ksid": ksid, "type": vt, "target_ttl": strconv.FormatUint(uint64(ttl), 8)},
	)
}

func statsCountOldTimeseries(ksid, vt string, ttl uint8) {
	go statsIncrement(
		"timeseries.count.old",
		map[string]string{"target_ksid": ksid, "type": vt, "target_ttl": strconv.FormatUint(uint64(ttl), 8)},
	)
}
