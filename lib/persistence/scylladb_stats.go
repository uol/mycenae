package persistence

import "time"

func (backend *scyllaPersistence) statsQuery(
	keyspace, column, operation string,
	d time.Duration,
) {
	tags := map[string]string{"keyspace": keyspace, "operation": operation}
	if column != "" {
		tags["column_family"] = column
	}
	go backend.statsIncrement("cassandra.query", tags)
	go backend.statsValueAdd(
		"cassandra.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func (backend *scyllaPersistence) statsQueryError(
	keyspace, column, operation string,
) {
	tags := map[string]string{"keyspace": keyspace, "operation": operation}
	if column != "" {
		tags["column_family"] = column
	}
	go backend.statsIncrement(
		"cassandra.query.error",
		tags,
	)
}

func (backend *scyllaPersistence) statsIncrement(
	metric string, tags map[string]string,
) {
	backend.stats.Increment("keyspace/persistence", metric, tags)
}

func (backend *scyllaPersistence) statsValueAdd(
	metric string, tags map[string]string, value float64,
) {
	backend.stats.ValueAdd("keyspace/persistence", metric, tags, value)
}
