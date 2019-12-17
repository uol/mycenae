package collector

import (
	"github.com/uol/gobol"
)

func (collector *Collector) saveValue(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.TTL]
	return collector.InsertPoint(
		ksid,
		packet.ID,
		packet.Timestamp,
		*packet.Message.Value,
	)
}

func (collector *Collector) saveText(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.TTL]
	return collector.InsertText(
		ksid,
		packet.ID,
		packet.Timestamp,
		packet.Message.Text,
	)
}
