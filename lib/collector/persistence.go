package collector

import (
	"fmt"
	"time"

	"github.com/uol/gobol/logh"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
)

func (collect *Collector) InsertPoint(ksid, tsid string, timestamp int64, value float64) gobol.Error {
	start := time.Now()
	var err error
	if err = collect.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_number_stamp (id, date, value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timestamp,
		value,
	).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_number_stamp")
		if logh.ErrorEnabled {
			collect.logger.Error().Str(constants.StringsFunc, "InsertPoint").Err(err).Send()
		}

		statsInsertFBerror(ksid, "ts_number_stamp")
		return errPersist("InsertPoint", err)
	}
	statsInsert(ksid, "ts_number_stamp", time.Since(start))
	return nil
}

func (collect *Collector) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {
	start := time.Now()
	var err error
	if err = collect.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_text_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timestamp,
		text,
	).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_text_stamp")
		if logh.ErrorEnabled {
			collect.logger.Error().Str(constants.StringsFunc, "InsertText").Err(err).Send()
		}
		statsInsertFBerror(ksid, "ts_text_stamp")
		return errPersist("InsertText", err)
	}
	statsInsert(ksid, "ts_text_stamp", time.Since(start))
	return nil
}

func (collect *Collector) CheckMetadata(index, tsType, id string) (bool, gobol.Error) {

	start := time.Now()
	ok, err := collect.metaStorage.CheckMetadata(index, tsType, id)
	if err != nil {
		statsIndexError(index, "all", "head")
		return false, errPersist("CheckMetadata", err)
	}
	statsIndex(index, "all", "head", time.Since(start))

	return ok, nil
}

func (collect *Collector) AddMetadata(collection string, metadata *metadata.Metadata) gobol.Error {
	start := time.Now()

	err := collect.metaStorage.AddDocument(collection, metadata)
	if err != nil {
		statsIndexError(collection, "document", "AddMetadata")
		return errPersist("AddMetadata", err)
	}
	statsIndex(collection, "document", "AddMetadata", time.Since(start))

	return nil
}
