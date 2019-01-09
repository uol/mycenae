package persistence

import "time"

func (backend *scylladb) statsQuery(
	keyspace, column, operation string,
	d time.Duration,
) {
	tags := map[string]string{"keyspace": keyspace, "operation": operation}
	if column != "" {
		tags["column_family"] = column
	}
	go backend.statsIncrement("scylla.query", tags)
	go backend.statsValueAdd(
		"scylla.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func (backend *scylladb) statsQueryError(
	keyspace, column, operation string,
) {
	tags := map[string]string{"keyspace": keyspace, "operation": operation}
	if column != "" {
		tags["column_family"] = column
	}
	go backend.statsIncrement(
		"scylla.query.error",
		tags,
	)
}

func (backend *scylladb) statsIncrement(
	metric string, tags map[string]string,
) {
	backend.stats.Increment("keyspace/persistence", metric, tags)
}

func (backend *scylladb) statsValueAdd(
	metric string, tags map[string]string, value float64,
) {
	backend.stats.ValueAdd("keyspace/persistence", metric, tags, value)
}
