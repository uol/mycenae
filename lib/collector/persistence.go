package collector

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	SECOND int64 = 1000000000
)

type persistence struct {
	cassandra   *gocql.Session
	metaStorage *metadata.Storage
}

func (persist *persistence) InsertPoint(ksid, tsid string, timestamp int64, value float64) gobol.Error {
	start := time.Now()
	var err error
	if err = persist.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_number_stamp (id, date, value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timestamp,
		value,
	).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_number_stamp")
		lf := []zapcore.Field{
			zap.String("package", "collector/persistence"),
			zap.String("func", "insertPoint"),
		}
		gblog.Error(err.Error(), lf...)
		statsInsertFBerror(ksid, "ts_number_stamp")
		return errPersist("InsertPoint", err)
	}
	statsInsert(ksid, "ts_number_stamp", time.Since(start))
	return nil
}

func (persist *persistence) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {
	start := time.Now()
	var err error
	if err = persist.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_text_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timestamp,
		text,
	).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_text_stamp")
		lf := []zapcore.Field{
			zap.String("package", "collector/persistence"),
			zap.String("func", "InsertText"),
		}
		gblog.Error(err.Error(), lf...)
		statsInsertFBerror(ksid, "ts_text_stamp")
		return errPersist("InsertText", err)
	}
	statsInsert(ksid, "ts_text_stamp", time.Since(start))
	return nil
}

func (persist *persistence) CheckMetadata(index, tsType, id string) (bool, gobol.Error) {

	start := time.Now()
	ok, err := persist.metaStorage.CheckMetadata(index, tsType, id)
	if err != nil {
		statsIndexError(index, "all", "head")
		return false, errPersist("CheckMetadata", err)
	}
	statsIndex(index, "all", "head", time.Since(start))

	return ok, nil
}

func (persist *persistence) SaveBulk(metadataMap map[string][]metadata.Metadata) gobol.Error {
	start := time.Now()

	for collection, metadatas := range metadataMap {
		err := persist.metaStorage.AddDocuments(collection, metadatas)
		if err != nil {
			statsIndexError(collection, "all", "bulk")
			return errPersist("SaveBulk", err)
		}
		statsIndex(collection, "all", "bulk", time.Since(start))
	}

	return nil
}
