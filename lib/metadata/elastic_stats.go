package metadata

import "time"

func (backend *elasticBackend) statsIndexError(index, estype, method string) {
	tags := map[string]string{
		"index":  index,
		"method": method,
	}
	if estype != "" {
		tags["type"] = estype
	}
	go backend.statsIncrement("elastic.request.error", tags)
}

func (backend *elasticBackend) statsIndex(
	index, estype, method string, duration time.Duration,
) {
	tags := map[string]string{"index": index, "method": method}
	if estype != "" {
		tags["type"] = estype
	}
	go backend.statsIncrement("elastic.request", tags)
	go backend.statsValueAdd(
		"elastic.request.duration", tags,
		float64(duration.Nanoseconds())/float64(time.Millisecond),
	)
}

func (backend *elasticBackend) statsIncrement(
	metric string, tags map[string]string,
) {
	backend.stats.Increment("keyspace/persistence", metric, tags)
}

func (backend *elasticBackend) statsValueAdd(
	metric string, tags map[string]string, value float64,
) {
	backend.stats.ValueAdd("keyspace/persistence", metric, tags, value)
}
