package memcached

import (
	"github.com/uol/mycenae/lib/tsstats"
	"time"
)

var (
	stats *tsstats.StatsTS
)

func statsError(oper string, namespace string) {
	go statsIncrement(
		"memcached.error",
		map[string]string{"bucket": namespace, "operation": oper},
	)
}

func statsSuccess(oper string, namespace string, d time.Duration) {
	go statsIncrement("bolt.query", map[string]string{"bucket": namespace, "operation": oper})
	go statsValueAdd(
		"memcached.duration",
		map[string]string{"bucket": namespace, "operation": oper},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsNotFound(namespace string) {
	go statsIncrement(
		"memcached.not_found",
		map[string]string{"bucket": namespace},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("memcached/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("memcached/persistence", metric, tags, v)
}
