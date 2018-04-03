package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collect *Collector) metaCoordinator(saveInterval time.Duration) {

	ticker := time.NewTicker(saveInterval)

	for {
		select {
		case <-ticker.C:

			if collect.metaPayload.Len() != 0 {

				collect.concBulk <- struct{}{}

				bulk := &bytes.Buffer{}

				err := collect.readMeta(bulk)
				if err != nil {
					lf := []zapcore.Field{
						zap.String("package", "collector"),
						zap.String("func", "metaCoordinator"),
						zap.String("action", "readMeta"),
					}
					gblog.Error(err.Error(), lf...)
					continue
				}

				go collect.saveBulk(bulk)

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

			if collect.metaPayload.Len() > collect.settings.MaxMetaBulkSize {

				collect.concBulk <- struct{}{}

				bulk := &bytes.Buffer{}

				err := collect.readMeta(bulk)
				if err != nil {
					lf := []zapcore.Field{
						zap.String("package", "collector"),
						zap.String("func", "metaCoordinator"),
						zap.String("action", "readMeta"),
					}
					gblog.Error(err.Error(), lf...)

					continue
				}

				go collect.saveBulk(bulk)
			}
		}
	}
}

func (collect *Collector) readMeta(bulk *bytes.Buffer) error {

	for {
		b, err := collect.metaPayload.ReadBytes(124)
		if err != nil {
			return err
		}

		b = b[:len(b)-1]

		_, err = bulk.Write(b)
		if err != nil {
			return err
		}

		if bulk.Len() >= collect.settings.MaxMetaBulkSize || collect.metaPayload.Len() == 0 {
			break
		}
	}

	return nil
}

func (collect *Collector) saveMeta(packet Point) {

	found := false

	var gerr gobol.Error

	ksts := fmt.Sprintf("%v|%v", packet.Keyset, packet.ID)

	if packet.Number {
		found, gerr = collect.keyspaceCache.GetTsNumber(ksts, collect.CheckTSID)
	} else {
		found, gerr = collect.keyspaceCache.GetTsText(ksts, collect.CheckTSID)
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

	var metricType, tagkType, tagvType, metaType string

	if packet.Number {
		metricType = "metric"
		tagkType = "tagk"
		tagvType = "tagv"
		metaType = "meta"
	} else {
		metricType = "metrictext"
		tagkType = "tagktext"
		tagvType = "tagvtext"
		metaType = "metatext"
	}

	idx := BulkType{
		ID: EsIndex{
			EsIndex: packet.Keyset,
			EsType:  metricType,
			EsID:    packet.Message.Metric,
		},
	}

	indexJSON, err := json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(indexJSON)
	collect.metaPayload.WriteString("\n")

	metric := EsMetric{
		Metric: packet.Message.Metric,
	}

	docJSON, err := json.Marshal(metric)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(docJSON)
	collect.metaPayload.WriteString("\n")

	cleanTags := []Tag{}

	for k, v := range packet.Message.Tags {

		idx := BulkType{
			ID: EsIndex{
				EsIndex: packet.Keyset,
				EsType:  tagkType,
				EsID:    k,
			},
		}

		indexJSON, err := json.Marshal(idx)

		if err != nil {
			return errMarshal("saveTsInfo", err)
		}

		collect.metaPayload.Write(indexJSON)
		collect.metaPayload.WriteString("\n")

		docTK := EsTagKey{
			Key: k,
		}

		docJSON, err := json.Marshal(docTK)
		if err != nil {
			return errMarshal("saveTsInfo", err)
		}

		collect.metaPayload.Write(docJSON)
		collect.metaPayload.WriteString("\n")

		idx = BulkType{
			ID: EsIndex{
				EsIndex: packet.Keyset,
				EsType:  tagvType,
				EsID:    v,
			},
		}

		indexJSON, err = json.Marshal(idx)
		if err != nil {
			return errMarshal("saveTsInfo", err)
		}

		collect.metaPayload.Write(indexJSON)
		collect.metaPayload.WriteString("\n")

		docTV := EsTagValue{
			Value: v,
		}

		docJSON, err = json.Marshal(docTV)
		if err != nil {
			return errMarshal("saveTsInfo", err)
		}

		collect.metaPayload.Write(docJSON)
		collect.metaPayload.WriteString("\n")

		cleanTags = append(cleanTags, Tag{Key: k, Value: v})
	}

	idx = BulkType{
		ID: EsIndex{
			EsIndex: packet.Keyset,
			EsType:  metaType,
			EsID:    packet.ID,
		},
	}

	indexJSON, err = json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(indexJSON)
	collect.metaPayload.WriteString("\n")

	docM := MetaInfo{
		ID:     packet.ID,
		Metric: packet.Message.Metric,
		Tags:   cleanTags,
	}

	docJSON, err = json.Marshal(docM)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(docJSON)
	collect.metaPayload.WriteString("\n")

	collect.metaPayload.WriteString("|")

	return nil
}

func (collect *Collector) saveBulk(boby io.Reader) {

	gerr := collect.persist.SaveBulkES(boby)
	if gerr != nil {
		lf := []zapcore.Field{
			zap.String("package", "collector"),
			zap.String("func", "saveBulk"),
		}
		gblog.Error(gerr.Error(), lf...)
	}

	<-collect.concBulk
}
