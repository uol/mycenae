package collector

import (
	"fmt"

	"github.com/uol/gobol"
)

func (collector *Collector) saveValue(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.TTL]
	return collector.persist.InsertPoint(
		ksid,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.Timestamp,
		*packet.Message.Value,
	)
}

func (collector *Collector) saveText(packet *Point) gobol.Error {
	ksid := collector.keyspaceTTLMap[packet.TTL]
	return collector.persist.InsertText(
		ksid,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.Timestamp,
		packet.Message.Text,
	)
}
