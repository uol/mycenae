package keyspace

import (
	"time"
)

func statsQueryError(ks, cf, oper string) {
	tags := map[string]string{"keyspace": ks, "operation": oper}
	if cf != "" {
		tags["column_family"] = cf
	}
	go statsIncrement(
		"cassandra.query.error",
		tags,
	)
}

func statsQuery(ks, cf, oper string, d time.Duration) {
	tags := map[string]string{"keyspace": ks, "operation": oper}
	if cf != "" {
		tags["column_family"] = cf
	}
	go statsIncrement("cassandra.query", tags)
	go statsValueAdd(
		"cassandra.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("keyspace/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("keyspace/persistence", metric, tags, v)
}
