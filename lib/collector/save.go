package collector

import (
	"github.com/uol/gobol"
)

func (collector *Collector) saveValue(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.Message.TTL]
	return collector.InsertPoint(
		ksid,
		packet.ID,
		packet.Message.Timestamp,
		*(packet.Message.Value),
	)
}

func (collector *Collector) saveText(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.Message.TTL]
	return collector.InsertText(
		ksid,
		packet.ID,
		packet.Message.Timestamp,
		packet.Message.Text,
	)
}
