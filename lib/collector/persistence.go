package collector

import (
	"fmt"
	"io"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

const (
	SECOND int64 = 1000000000
)

type persistence struct {
	cassandra *gocql.Session
	esearch   *rubber.Elastic
}

func (persist *persistence) InsertPoint(ksid, tsid string, timestamp int64, value float64) gobol.Error {
	start := time.Now()
	var err error
	if err = persist.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_number_stamp (id, date, value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timestamp,
		value,
	).RoutingKey([]byte(tsid)).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_number_stamp")
		gblog.WithFields(
			logrus.Fields{
				"package": "collector/persistence",
				"func":    "insertPoint",
			},
		).Error(err)
		statsInsertFBerror(ksid, "ts_number_stamp")
		return errPersist("InsertPoint", err)
	}
	statsInsert(ksid, "ts_number_stamp", time.Since(start))
	return nil
}

func (persist *persistence) InsertTUUIDpoint(ksid, tsid string, timeU gocql.UUID, value float64) gobol.Error {
	start := time.Now()
	var err error
	if err = persist.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_number (id, date , value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timeU,
		value,
	).RoutingKey([]byte(tsid)).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_number")
		gblog.WithFields(
			logrus.Fields{
				"package": "collector/persistence",
				"func":    "InsertTUUIDpoint",
			},
		).Error(err)
		statsInsertFBerror(ksid, "ts_number")
		return errPersist("InsertTUUIDpoint", err)
	}
	statsInsert(ksid, "ts_number", time.Since(start))
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
	).RoutingKey([]byte(tsid)).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_text_stamp")
		gblog.WithFields(
			logrus.Fields{
				"package": "collector/persistence",
				"func":    "InsertText",
			},
		).Error(err)
		statsInsertFBerror(ksid, "ts_text_stamp")
		return errPersist("InsertText", err)
	}
	statsInsert(ksid, "ts_text_stamp", time.Since(start))
	return nil
}

func (persist *persistence) InsertTUUIDtext(ksid, tsid string, timeU gocql.UUID, text string) gobol.Error {
	start := time.Now()
	var err error
	if err = persist.cassandra.Query(
		fmt.Sprintf(`INSERT INTO %v.ts_text (id, date , value) VALUES (?, ?, ?)`, ksid),
		tsid,
		timeU,
		text,
	).RoutingKey([]byte(tsid)).Exec(); err != nil {
		statsInsertQerror(ksid, "ts_text")
		gblog.WithFields(
			logrus.Fields{
				"package": "collector/persistence",
				"func":    "InsertTUUIDtext",
			},
		).Error(err)
		statsInsertFBerror(ksid, "ts_text")
		return errPersist("InsertTUUIDtext", err)
	}
	statsInsert(ksid, "ts_text", time.Since(start))
	return nil

}

func (persist *persistence) HeadMetaFromES(index, eType, id string) (int, gobol.Error) {
	start := time.Now()
	respCode, err := persist.esearch.GetHead(index, eType, id)
	if err != nil {
		statsIndexError(index, eType, "head")
		return 0, errPersist("HeadMetaFromES", err)
	}
	statsIndex(index, eType, "head", time.Since(start))
	return respCode, nil
}

func (persist *persistence) SendErrorToES(index, eType, id string, doc StructV2Error) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.Put(index, eType, id, doc)
	if err != nil {
		statsIndexError(index, eType, "put")
		return errPersist("SendErrorToES", err)
	}
	statsIndex(index, eType, "PUT", time.Since(start))
	return nil
}

func (persist *persistence) SaveBulkES(body io.Reader) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.PostBulk(body)
	if err != nil {
		statsIndexError("", "", "bulk")
		return errPersist("SaveBulkES", err)
	}
	statsIndex("", "", "bulk", time.Since(start))
	return nil
}
