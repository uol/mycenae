package collector

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

const (
	cMetaTypeNumber string = "meta"
	cMetaTypeText   string = "metatext"
)

func (collect *Collector) saveMeta(packet *Point) gobol.Error {

	found := false

	var gerr gobol.Error

	if packet.Number {
		found, gerr = collect.CheckMetadata(packet.Message.Keyset, cMetaTypeNumber, packet.ID, packet.HashID)
	} else {
		found, gerr = collect.CheckMetadata(packet.Message.Keyset, cMetaTypeText, packet.ID, packet.HashID)
	}

	if gerr != nil {
		statsLostMeta(packet.Message.Keyset)
		return gerr
	}

	var metaType string
	if packet.Number {
		metaType = cMetaTypeNumber
	} else {
		metaType = cMetaTypeText
	}

	if !found {
		statsCountNewTimeseries(packet.Message.Keyset, metaType, packet.Message.TTL)

		var tagKeys, tagValues []string
		for _, tag := range packet.Message.Tags {
			if tag.Name != constants.StringsKSID {
				tagKeys = append(tagKeys, tag.Name)
				tagValues = append(tagValues, tag.Value)
			}
		}

		metadata := &metadata.Metadata{
			ID:       packet.ID,
			Metric:   packet.Message.Metric,
			MetaType: metaType,
			TagKey:   tagKeys,
			TagValue: tagValues,
		}

		collect.AddMetadata(packet.Message.Keyset, metadata)
	} else {
		statsCountOldTimeseries(packet.Message.Keyset, metaType, packet.Message.TTL)
	}

	return nil
}
