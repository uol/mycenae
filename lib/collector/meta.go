package collector

import (
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/util"
	"github.com/uol/mycenae/lib/metadata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collect *Collector) cloneMetadataMap() map[string][]metadata.Metadata {

	cloned := map[string][]metadata.Metadata{}

	if util.GetSyncMapSize(&collect.metadataMap) > 0 {
		collect.metadataMap.Range(func(k, v interface{}) bool {
			cloned[k.(string)] = v.([]metadata.Metadata)
			collect.metadataMap.Delete(k)

			return true
		})
	}

	return cloned
}

func (collect *Collector) metaCoordinator(saveInterval time.Duration) {

	go func() {
		for {
			<-time.After(saveInterval)

			if util.GetSyncMapSize(&collect.metadataMap) != 0 {
				collect.concBulk <- struct{}{}
				go collect.saveBulk()
			}
		}
	}()

	go func() {
		for {
			p := <-collect.metaChan

			gerr := collect.generateBulk(p)
			if gerr != nil {
				lf := []zapcore.Field{
					zap.String("package", "collector"),
					zap.String("func", "metaCoordinator"),
					zap.String("action", "generateBulk"),
				}
				gblog.Error(gerr.Error(), lf...)
			}

			if util.GetSyncMapSize(&collect.metadataMap) > collect.settings.MaxMetaBulkSize {
				collect.concBulk <- struct{}{}
				go collect.saveBulk()
			}
		}
	}()
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

	if _, ok := collect.metadataMap.Load(packet.Keyset); !ok {
		collect.metadataMap.Store(packet.Keyset, []metadata.Metadata{})
	}

	var tagKeys, tagValues []string
	for key, value := range packet.Message.Tags {
		if key != "ksid" {
			tagKeys = append(tagKeys, key)
			tagValues = append(tagValues, value)
		}
	}

	newItem := metadata.Metadata{
		ID:       packet.ID,
		Metric:   packet.Message.Metric,
		MetaType: metaType,
		TagKey:   tagKeys,
		TagValue: tagValues,
	}

	metadataInterface, ok := collect.metadataMap.Load(packet.Keyset)

	var metadatas []metadata.Metadata
	if !ok {
		metadatas = []metadata.Metadata{}
	} else {
		metadatas = metadataInterface.([]metadata.Metadata)
	}

	metadatas = append(metadatas, newItem)

	collect.metadataMap.Store(packet.Keyset, metadatas)

	return nil
}

func (collect *Collector) saveBulk() {

	gerr := collect.persist.SaveBulk(collect.cloneMetadataMap())
	if gerr != nil {
		lf := []zapcore.Field{
			zap.String("package", "collector"),
			zap.String("func", "saveBulk"),
		}
		gblog.Error(gerr.Error(), lf...)
	}

	<-collect.concBulk
}
