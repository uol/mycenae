package plot

import "time"

func (persist *persistence) statsSelectQerror(ks, cf string) {
	go persist.statsIncrement(
		"scylla.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func (persist *persistence) statsSelectFerror(ks, cf string) {
	go persist.statsIncrement(
		"scylla.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func (persist *persistence) statsSelect(ks, cf string, d time.Duration, countRows int) {
	tags := map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"}
	go persist.statsIncrement("scylla.query", tags)
	go persist.statsValueAdd(
		"scylla.query.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
	go persist.statsValueMax("scylla.query.max.rows", tags, float64(countRows))
}

func (persist *persistence) statsIncrement(metric string, tags map[string]string) {
	persist.stats.Increment(cPackage, metric, tags)
}

func (persist *persistence) statsValueAdd(metric string, tags map[string]string, v float64) {
	persist.stats.ValueAdd(cPackage, metric, tags, v)
}

func (persist *persistence) statsValueMax(metric string, tags map[string]string, v float64) {
	persist.stats.ValueMax(cPackage, metric, tags, v)
}
