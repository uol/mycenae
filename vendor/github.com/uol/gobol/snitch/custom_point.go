package snitch

import (
	"math"
	"sync/atomic"
	"time"
)

//CustomPoint is the base structure used by Point, CustomPoint should be used if you need more control over your stats.
type CustomPoint struct {
	metric      string
	tags        map[string]string
	value       uint64
	valNull     uint32
	timestamp   int64
	count       int64
	interval    string
	aggregation string
	keepValue   bool
	sendOnNull  bool
	pre         PreTransform
	post        PostTransform
	sender      chan message
}

// ValueAdd adds v to value and increment the counter.
func (p *CustomPoint) ValueAdd(v float64) {

	for {
		ob := atomic.LoadUint64(&p.value)
		nb := math.Float64bits(math.Float64frombits(ob) + v)
		if atomic.CompareAndSwapUint64(&p.value, ob, nb) {
			break
		}
	}

	atomic.AddInt64(&p.count, 1)
	atomic.StoreUint32(&p.valNull, 0)
}

// SetCount sets count to i.
func (p *CustomPoint) SetCount(i int64) {
	atomic.StoreInt64(&p.count, i)
}

// SetValue sets value to v.
func (p *CustomPoint) SetValue(v float64) {

	atomic.StoreUint64(&p.value, math.Float64bits(v))
	atomic.StoreUint32(&p.valNull, 0)
}

// SetTimestamp sets timestamp to t.
func (p *CustomPoint) SetTimestamp(t int64) {
	atomic.StoreInt64(&p.timestamp, t)
}

// GetValue return the current value.
func (p *CustomPoint) GetValue() float64 {
	return math.Float64frombits(atomic.LoadUint64(&p.value))
}

// GetCount return the current count.
func (p *CustomPoint) GetCount() int64 {
	return atomic.LoadInt64(&p.count)
}

// GetTimestamp returns the current timestamp.
func (p *CustomPoint) GetTimestamp() int64 {
	return atomic.LoadInt64(&p.timestamp)
}

// SetValueNull sets the value to zero and the valNull boolean to true.
func (p *CustomPoint) SetValueNull() {
	atomic.StoreUint64(&p.value, 0)
	atomic.StoreUint32(&p.valNull, 1)
}

// IsValueNull informs if the value is null or not.
func (p *CustomPoint) IsValueNull() bool {
	if atomic.LoadUint32(&p.valNull) == 0 {
		return false
	}

	return true
}

// Run takes care of the logic for sending points
func (p *CustomPoint) Run() {
	send := p.pre(p)

	ts := p.GetTimestamp()
	if ts == 0 {
		ts = time.Now().UnixNano() / int64(time.Millisecond)
	}

	if send {
		p.sender <- message{
			Metric:    p.metric,
			Tags:      p.tags,
			Value:     p.GetValue(),
			Timestamp: ts,
		}
	}
	p.post(p)
}
