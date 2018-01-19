package keyset

import "time"

func (ks *KeySet) statsIndexError(i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go ks.statsIncrement("elastic.request.error", tags)
}

func (ks *KeySet) statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go ks.statsIncrement("elastic.request", tags)
	go ks.statsValueAdd(
		"elastic.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func (ks *KeySet) statsIncrement(metric string, tags map[string]string) {
	ks.stats.Increment("keyset/index", metric, tags)
}

func (ks *KeySet) statsValueAdd(metric string, tags map[string]string, v float64) {
	ks.stats.ValueAdd("keyset/index", metric, tags, v)
}