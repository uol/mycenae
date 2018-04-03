package plot

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (persist *persistence) GetTS(keyspace, key string, start, end int64, ms bool) (Pnts, int, gobol.Error) {

	return persist.getTStamp(keyspace, key, start, end, ms)
}

func (persist *persistence) getTStamp(keyspace, key string, start, end int64, ms bool) ([]Pnt, int, gobol.Error) {
	track := time.Now()
	start--
	end++

	var date int64
	var value float64
	var err error

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT date, value FROM %v.ts_number_stamp WHERE id= ? AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
		),
		key,
		start,
		end,
	).RoutingKey([]byte(key)).Iter()

	points := []Pnt{}
	var count int

	for iter.Scan(&date, &value) {
		count++
		if !ms {
			date = (date / 1000) * 1000
		}
		point := Pnt{
			Date:  date,
			Value: value,
		}
		points = append(points, point)
	}

	if err = iter.Close(); err != nil {
		fields := []zapcore.Field{
			zap.String("package", "plot/persistence"),
			zap.String("func", "getTStamp"),
		}
		gblog.Error(err.Error(), fields...)

		if err == gocql.ErrNotFound {
			statsSelect(keyspace, "ts_number_stamp", time.Since(track))
			return []Pnt{}, 0, errNoContent("getTStamp")
		}

		statsSelectQerror(keyspace, "ts_number_stamp")
		return []Pnt{}, 0, errPersist("getTStamp", err)
	}
	statsSelect(keyspace, "ts_number_stamp", time.Since(track))
	return points, count, nil
}

func (persist *persistence) fuseNumber(countF, countS int, first, second []Pnt) []Pnt {

	fused := make(Pnts, countF+countS)
	var i, j, k int

	for i < countF && j < countS {
		if first[i].Date <= second[j].Date {
			fused[k] = first[i]
			i++
		} else {
			fused[k] = second[j]
			j++
		}
		k++
	}
	if i < countF {
		for p := i; p < countF; p++ {
			fused[k] = first[p]
			k++
		}
	} else {
		for p := j; p < countS; p++ {
			fused[k] = second[p]
			k++
		}
	}

	return fused
}
