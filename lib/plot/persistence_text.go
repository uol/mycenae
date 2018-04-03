package plot

import (
	"fmt"
	"regexp"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (persist *persistence) GetTST(
	keyspace,
	key string,
	start,
	end int64,
	search *regexp.Regexp,
) (TextPnts, int, gobol.Error) {

	return persist.getTSTstamp(keyspace, key, start, end, search)
}

func (persist *persistence) getTSTstamp(
	keyspace,
	key string,
	start,
	end int64,
	search *regexp.Regexp,
) ([]TextPnt, int, gobol.Error) {
	track := time.Now()
	start--
	end++

	var date int64
	var value string
	var err error

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT date, value FROM %v.ts_text_stamp WHERE id= ? AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
		),
		key,
		start,
		end,
	).RoutingKey([]byte(key)).Iter()

	points := []TextPnt{}
	var count int

	for iter.Scan(&date, &value) {
		add := true

		if search != nil && !search.MatchString(value) {
			add = false
		}

		if add {
			count++
			point := TextPnt{
				Date:  date,
				Value: value,
			}
			points = append(points, point)
		}
	}

	if err = iter.Close(); err != nil {
		fields := []zapcore.Field{
			zap.String("package", "plot/persistence"),
			zap.String("func", "getTSTstamp"),
		}
		gblog.Error(err.Error(), fields...)

		if err == gocql.ErrNotFound {
			return []TextPnt{}, 0, errNoContent("getTSTstamp")
		}

		statsSelectFerror(keyspace, "ts_text_stamp")
		return []TextPnt{}, 0, errPersist("getTSTstamp", err)
	}
	statsSelect(keyspace, "ts_text_stamp", time.Since(track))
	return points, count, nil
}

func (persist *persistence) fuseText(countF, countS int, first, second []TextPnt) []TextPnt {

	fused := make(TextPnts, countF+countS)
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
