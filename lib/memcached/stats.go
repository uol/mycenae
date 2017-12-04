package memcached

import (
	"time"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	stats *tsstats.StatsTS
)

func statsError(oper string, bucket string) {
	go statsIncrement(
		"memcached.error",
		map[string]string{"bucket": bucket, "operation": oper},
	)
}

func statsSuccess(oper string, bucket string, d time.Duration) {
	go statsIncrement("bolt.query", map[string]string{"bucket": bucket, "operation": oper})
	go statsValueAdd(
		"memcached.duration",
		map[string]string{"bucket": bucket, "operation": oper},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsNotFound(bucket string) {
	go statsIncrement(
		"memcached.notfound",
		map[string]string{"bucket": bucket},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("memcached/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("memcached/persistence", metric, tags, v)
}
