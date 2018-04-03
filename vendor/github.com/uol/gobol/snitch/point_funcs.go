package snitch

// Increment is a shorthand to ValueAdd with a value of one
func (st *Stats) Increment(
	metricName string,
	tags map[string]string,
	interval string,
	keep, nullable bool,
) error {
	return st.ValueAdd(metricName, tags, "sum", interval, keep, nullable, 1)
}

// ValueAdd looks on the stats map for the point with the given name and adds v to its value.
func (st *Stats) ValueAdd(
	metricName string,
	tags map[string]string,
	aggregation, interval string,
	keep, nullable bool, v float64,
) error {
	p, err := st.getPoint(metricName, tags, aggregation, interval, keep, nullable)
	if err != nil {
		return err
	}
	switch p.aggregation {
	case "avg":
		p.ValueAdd(v)
	case "sum":
		p.ValueAdd(v)
	case "max":
		if p.GetValue() < v {
			p.SetValue(v)
		}
	case "min":
		if p.IsValueNull() {
			p.SetValue(v)
		} else if p.GetValue() > v {
			p.SetValue(v)
		}
	default:
		p.ValueAdd(v)
	}
	return nil
}

// SetValue looks on the stats map for the point with the given name and set its
// value to v
func (st *Stats) SetValue(
	metricName string,
	tags map[string]string,
	interval string,
	keep, nullable bool, v float64,
) error {
	p, err := st.getPoint(metricName, tags, "sum", interval, keep, nullable)
	if err != nil {
		return err
	}
	p.SetValue(v)
	return nil
}
