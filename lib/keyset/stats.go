package keyset

import "time"

func (ks *KeySet) statsIndexError(index, method string) {
	tags := map[string]string{"index": index, "method": method}
	go ks.statsIncrement("solr.request.error", tags)
}

func (ks *KeySet) statsIndex(index, method string, d time.Duration) {
	tags := map[string]string{"index": index, "method": method}

	go ks.statsIncrement("solr.request", tags)
	go ks.statsValueAdd(
		"solr.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func (ks *KeySet) statsIncrement(metric string, tags map[string]string) {
	ks.stats.Increment("keysets", metric, tags)
}

func (ks *KeySet) statsValueAdd(metric string, tags map[string]string, v float64) {
	ks.stats.ValueAdd("keysets", metric, tags, v)
}
