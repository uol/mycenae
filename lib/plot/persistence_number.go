package plot

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (persist *persistence) GetTS(keyspace string, keys []string, start, end int64, ms bool) (map[string][]Pnt, gobol.Error) {

	track := time.Now()
	start--
	end++

	var tsid string
	var date int64
	var value float64
	var err error

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT id, date, value FROM %v.ts_number_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
			persist.buildInGroup(keys),
		),
		start,
		end,
	).Iter()

	tsMap := map[string][]Pnt{}
	countRows := 0

	for iter.Scan(&tsid, &date, &value) {

		if !ms {
			date = (date / 1000) * 1000
		}

		point := Pnt{
			TSID:  tsid,
			Date:  date,
			Value: value,
		}

		tsMap[tsid] = append(tsMap[tsid], point)

		countRows++
	}

	if err = iter.Close(); err != nil {
		fields := []zapcore.Field{
			zap.String("package", "plot/persistence"),
			zap.String("func", "getTS"),
		}
		gblog.Error(err.Error(), fields...)

		if err == gocql.ErrNotFound {
			statsSelect(keyspace, "ts_number_stamp", time.Since(track), countRows)
			return map[string][]Pnt{}, errNoContent("getTS")
		}

		statsSelectQerror(keyspace, "ts_number_stamp")
		return map[string][]Pnt{}, errPersist("getTS", err)
	}

	statsSelect(keyspace, "ts_number_stamp", time.Since(track), countRows)

	return tsMap, nil
}
