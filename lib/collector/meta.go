package collector

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
)

func (collect *Collector) saveMeta(packet *Point) gobol.Error {

	found := false

	var gerr gobol.Error

	if packet.Number {
		found, gerr = collect.persist.CheckMetadata(packet.Keyset, "meta", packet.ID)
	} else {
		found, gerr = collect.persist.CheckMetadata(packet.Keyset, "metatext", packet.ID)
	}

	if gerr != nil {
		statsLostMeta()
		return gerr
	}

	var metaType string
	if packet.Number {
		metaType = "meta"
	} else {
		metaType = "metatext"
	}

	if !found {
		go statsCountNewTimeseries(packet.Keyset, metaType, packet.TTL)

		var tagKeys, tagValues []string
		for key, value := range packet.Message.Tags {
			if key != "ksid" {
				tagKeys = append(tagKeys, key)
				tagValues = append(tagValues, value)
			}
		}

		metadata := &metadata.Metadata{
			ID:       packet.ID,
			Metric:   packet.Message.Metric,
			MetaType: metaType,
			TagKey:   tagKeys,
			TagValue: tagValues,
		}

		collect.persist.AddMetadata(packet.Keyset, metadata)
	} else {
		go statsCountOldTimeseries(packet.Keyset, metaType, packet.TTL)
	}

	return nil
}
