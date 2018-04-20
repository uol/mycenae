package metadata

import "time"

/**
* Has functions to send data to the timeseries itself.
* @author rnojiri
**/

func (sb *SolrBackend) statsCollectionError(collection, action, metric string) {
	tags := map[string]string{
		"collection": collection,
		"action":     action,
	}
	go sb.statsIncrement(metric, tags)
}

func (sb *SolrBackend) statsCollectionAction(collection, action, metric string, duration time.Duration) {
	tags := map[string]string{
		"collection": collection,
		"action":     action,
	}
	go sb.statsIncrement(metric, tags)
	go sb.statsValueAdd(
		"solr.collection.duration", tags,
		float64(duration.Nanoseconds())/float64(time.Millisecond),
	)
}

func (sb *SolrBackend) statsIncrement(metric string, tags map[string]string) {
	sb.stats.Increment("metadata", metric, tags)
}

func (sb *SolrBackend) statsValueAdd(metric string, tags map[string]string, value float64) {
	sb.stats.ValueAdd("metadata", metric, tags, value)
}
