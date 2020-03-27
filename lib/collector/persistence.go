package collector

import (
	"fmt"
	"time"

	"github.com/uol/logh"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

const (
	fmtInsertNumberQuery string = `INSERT INTO %v.ts_number_stamp (id, date, value) VALUES (?, ?, ?)`
	fmtInsertTextQuery   string = `INSERT INTO %v.ts_text_stamp (id, date , value) VALUES (?, ?, ?)`
	tableNumberStamp     string = "ts_number_stamp"
	tableTextStamp       string = "ts_text_stamp"
)

func (collect *Collector) InsertPoint(ksid, tsid string, timestamp int64, value float64) gobol.Error {

	start := time.Now()

	var err error
	if err = collect.cassandra.Query(
		fmt.Sprintf(fmtInsertNumberQuery, ksid),
		tsid,
		timestamp,
		value,
	).Exec(); err != nil {
		statsInsertQueryError(ksid, tableNumberStamp)
		if logh.ErrorEnabled {
			collect.logger.Error().Err(err).Str(constants.StringsFunc, "InsertPoint").Str("tsid", tsid).Int64("timestamp", timestamp).Float64("value", value).Str("ksid", ksid).Send()
		}

		statsInsertRollback(ksid, tableNumberStamp)
		return errPersist("InsertPoint", err)
	}

	statsInsertQuery(ksid, tableNumberStamp, time.Since(start))

	return nil
}

func (collect *Collector) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {

	start := time.Now()

	var err error
	if err = collect.cassandra.Query(
		fmt.Sprintf(fmtInsertTextQuery, ksid),
		tsid,
		timestamp,
		text,
	).Exec(); err != nil {
		statsInsertQueryError(ksid, tableTextStamp)
		if logh.ErrorEnabled {
			collect.logger.Error().Err(err).Str(constants.StringsFunc, "InsertText").Str("tsid", tsid).Int64("timestamp", timestamp).Str("text", text).Str("ksid", ksid).Send()
		}
		statsInsertRollback(ksid, tableTextStamp)
		return errPersist("InsertText", err)
	}

	statsInsertQuery(ksid, tableTextStamp, time.Since(start))

	return nil
}

const funcCheckMetadata string = "CheckMetadata"

// CheckMetadata - checks for the metadata existence
func (collect *Collector) CheckMetadata(index, tsType, id string, idByte []byte) (bool, gobol.Error) {

	ok, err := collect.metaStorage.CheckMetadata(index, tsType, id, idByte)
	if err != nil {
		return false, errPersist(funcCheckMetadata, err)
	}

	return ok, nil
}

const funcAddMetadata string = "AddMetadata"

// AddMetadata - adds a new document metadata
func (collect *Collector) AddMetadata(collection string, m *metadata.Metadata) gobol.Error {

	err := collect.metaStorage.AddDocument(collection, m)
	if err != nil {
		return errPersist(funcAddMetadata, err)
	}

	return nil
}
