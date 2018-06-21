package collector

import (
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collect *Collector) cloneMetadataMap() map[string][]metadata.Metadata {

	cloned := map[string][]metadata.Metadata{}

	if len(collect.metadataMap) > 0 {
		for k, v := range collect.metadataMap {
			cloned[k] = v
		}

		collect.metadataMap = map[string][]metadata.Metadata{}
	}

	return cloned
}

func (collect *Collector) metaCoordinator(saveInterval time.Duration) {

	ticker := time.NewTicker(saveInterval)

	for {
		select {
		case <-ticker.C:

			if len(collect.metadataMap) != 0 {
				collect.concBulk <- struct{}{}
				go collect.saveBulk(collect.cloneMetadataMap())
			}

		case p := <-collect.metaChan:

			gerr := collect.generateBulk(p)
			if gerr != nil {
				lf := []zapcore.Field{
					zap.String("package", "collector"),
					zap.String("func", "metaCoordinator"),
					zap.String("action", "generateBulk"),
				}
				gblog.Error(gerr.Error(), lf...)
			}

			if len(collect.metadataMap) > collect.settings.MaxMetaBulkSize {
				collect.concBulk <- struct{}{}
				go collect.saveBulk(collect.cloneMetadataMap())
			}
		}
	}
}

func (collect *Collector) checkMetadata() {

}

func (collect *Collector) saveMeta(packet Point) {

	found := false

	var gerr gobol.Error

	if packet.Number {
		found, gerr = collect.persist.CheckMetadata(packet.Keyset, "meta", packet.ID)
	} else {
		found, gerr = collect.persist.CheckMetadata(packet.Keyset, "metatext", packet.ID)
	}
	if gerr != nil {
		lf := []zapcore.Field{
			zap.String("package", "collector"),
			zap.String("func", "saveMeta"),
		}
		gblog.Warn(gerr.Error(), lf...)
		collect.errMutex.Lock()
		collect.errorsSinceLastProbe++
		collect.errMutex.Unlock()
	}

	if !found {
		collect.metaChan <- packet
		statsBulkPoints()
	}

}

func (collect *Collector) generateBulk(packet Point) gobol.Error {

	var metaType string
	if packet.Number {
		metaType = "meta"
	} else {
		metaType = "metatext"
	}

	exists, err := collect.persist.metaStorage.CheckMetadata(packet.Keyset, metaType, packet.ID)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "collector"),
			zap.String("func", "generateBulk"),
		}
		gblog.Error(err.Error(), lf...)
	}

	if exists {
		statsCountOldTimeseries(packet.Keyset, metaType, packet.TTL)
		return nil
	}
	
	statsCountNewTimeseries(packet.Keyset, metaType, packet.TTL)

	if _, ok := collect.metadataMap[packet.Keyset]; !ok {
		collect.metadataMap[packet.Keyset] = []metadata.Metadata{}
	}

	var tagKeys, tagValues []string
	for key, value := range packet.Message.Tags {
		if key != "ksid" {
			tagKeys = append(tagKeys, key)
			tagValues = append(tagValues, value)
		}
	}

	metadata := metadata.Metadata{
		ID:       packet.ID,
		Metric:   packet.Message.Metric,
		MetaType: metaType,
		TagKey:   tagKeys,
		TagValue: tagValues,
	}

	collect.metadataMap[packet.Keyset] = append(collect.metadataMap[packet.Keyset], metadata)

	return nil
}

func (collect *Collector) saveBulk(metadataMap map[string][]metadata.Metadata) {

	gerr := collect.persist.SaveBulk(metadataMap)
	if gerr != nil {
		lf := []zapcore.Field{
			zap.String("package", "collector"),
			zap.String("func", "saveBulk"),
		}
		gblog.Error(gerr.Error(), lf...)
	}

	<-collect.concBulk
}
