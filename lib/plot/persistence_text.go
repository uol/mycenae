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

func (persist *persistence) GetTST(keyspace string, keys []string, start, end int64, search *regexp.Regexp) (map[string][]TextPnt, gobol.Error) {

	track := time.Now()
	start--
	end++

	var tsid string
	var date int64
	var value string
	var err error

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT id, date, value FROM %v.ts_text_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
			persist.buildInGroup(keys),
		),
		start,
		end,
	).Iter()

	tsMap := map[string][]TextPnt{}

	for iter.Scan(&tsid, &date, &value) {
		add := true

		if search != nil && !search.MatchString(value) {
			add = false
		}

		if add {
			point := TextPnt{
				Date:  date,
				Value: value,
			}
			tsMap[tsid] = append(tsMap[tsid], point)
		}
	}

	if err = iter.Close(); err != nil {
		fields := []zapcore.Field{
			zap.String("package", "plot/persistence"),
			zap.String("func", "getTST"),
		}
		gblog.Error(err.Error(), fields...)

		if err == gocql.ErrNotFound {
			return map[string][]TextPnt{}, errNoContent("getTST")
		}

		statsSelectFerror(keyspace, "ts_text_stamp")
		return map[string][]TextPnt{}, errPersist("getTST", err)
	}

	statsSelect(keyspace, "ts_text_stamp", time.Since(track))

	return tsMap, nil
}
