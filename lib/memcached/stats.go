package memcached

import (
	"github.com/uol/gobol/snitch"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
)

// MetricsCollector - implements the interface zencached.MetricsCollector
type MetricsCollector struct {

	// must be replaced with timeline
	stats *snitch.Stats
}

// send - unifies
func (mc *MetricsCollector) send(operation, metric string, value float64, tags ...string) {

	tagMap := map[string]string{}
	for i := 0; i < len(tags); i += 2 {
		tagMap[tags[i]] = tags[i+1]
	}

	go func() {
		err := mc.stats.ValueAdd(metric, tagMap, operation, "@every 10s", false, false, value)
		if err != nil {
			if logh.ErrorEnabled {
				logh.Error().Str(constants.StringsPKG, "memcached").Str(constants.StringsFunc, "Maximum").Str("metric", metric).Err(err).Send()
			}
		}
	}()
}

// Count - does the count operation
func (mc *MetricsCollector) Count(metric string, value float64, tags ...string) {

	mc.send("sum", metric, value, tags...)
}

// Maximum - does the max operation
func (mc *MetricsCollector) Maximum(metric string, value float64, tags ...string) {

	mc.send("max", metric, value, tags...)
}
