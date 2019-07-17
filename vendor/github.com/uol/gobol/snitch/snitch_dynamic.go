package snitch

import (
	"errors"
	"sort"
	"strings"

	"github.com/robfig/cron"

	"go.uber.org/zap"
)

func keyFromMetricID(metric string, tags map[string]string) string {
	merged := []string{}
	for k, v := range tags {
		s := make([]byte, len(k)+len(v)+1)
		copy(s, k)
		copy(s[len(k):], "=")
		copy(s[len(k)+1:], v)

		merged = append(merged, string(s))
	}
	sort.Strings(merged)
	merged = append(merged, metric)
	return strings.Join(merged, ", ")
}

var aggregationsContants = map[string]bool{
	"avg": true,
	"min": true,
	"max": true,
	"sum": true,
}

func (st *Stats) getPoint(
	metricName string,
	tags map[string]string,
	aggregation, interval string,
	keep, nullable bool,
) (*CustomPoint, error) {

	key := keyFromMetricID(metricName, tags)
	st.mtx.RLock()
	if metric, ok := st.points.Load(key); ok {
		st.mtx.RUnlock()
		return metric.(*CustomPoint), nil
	}
	st.mtx.RUnlock()

	if _, err := cron.Parse(interval); err != nil {
		return nil, err
	}
	if _, ok := aggregationsContants[aggregation]; !ok {
		return nil, errors.New("Unknown aggregation")
	}

	metric := &CustomPoint{
		metric:      metricName,
		tags:        make(map[string]string),
		aggregation: aggregation,
		keepValue:   keep,
		sendOnNull:  nullable,
		valNull:     1,
		interval:    interval,
		sender:      st.receiver,
		pre: func(p *CustomPoint) bool {
			if !p.sendOnNull && p.IsValueNull() {
				return false
			}
			switch p.aggregation {
			case "avg":
				y := p.GetValue()
				x := p.GetCount()
				if y != 0 && x != 0 {
					p.SetValue(y / float64(x))
				}
			}
			return true
		},
		post: func(p *CustomPoint) {

			if st.raiseDebugVerbosity {
				st.logger.Debug(
					"collected",
					zap.String("metric", p.metric),
					zap.String("interval", p.interval),
					zap.Float64("value", p.GetValue()),
					zap.Bool("null", p.IsValueNull()),
				)
			}

			if p.aggregation != "" || !p.keepValue {
				p.SetValue(0)
				p.SetCount(0)
				p.SetValueNull()
			}
		},
	}

	for k, v := range st.tags {
		metric.tags[k] = v
	}
	for k, v := range tags {
		metric.tags[k] = v
	}

	st.mtx.Lock()
	st.points.Store(key, metric)
	st.mtx.Unlock()

	st.cron.AddJob(interval, metric)
	return metric, nil
}
