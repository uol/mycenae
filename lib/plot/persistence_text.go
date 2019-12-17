package plot

import (
	"fmt"
	"regexp"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"
)

func (persist *persistence) GetTST(keyspace string, keys []string, start, end int64, search *regexp.Regexp, allowFullFetch bool, maxBytesLimit uint32, keyset string) (map[string][]TextPnt, uint32, gobol.Error) {

	track := time.Now()
	start--
	end++

	var tsid string
	var date int64
	var value string
	var err error
	var numBytes uint32
	idsGroup := persist.buildInGroup(keys)

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT id, date, value FROM %v.ts_text_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
			idsGroup,
		),
		start,
		end,
	).Iter()

	tsMap := map[string][]TextPnt{}
	countRows := 0
	limitReached := false

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

			if _, ok := tsMap[tsid]; !ok {
				numBytes += uint32(persist.getStringSize(tsid))
			}

			tsMap[tsid] = append(tsMap[tsid], point)

			numBytes += uint32(persist.constPartBytesFromTextPoint + persist.getStringSize(value))

			if !allowFullFetch && numBytes >= maxBytesLimit {
				limitReached = true
				break
			}

			countRows++
		}
	}

	go persist.statsValueAdd(
		"scylla.query.bytes",
		map[string]string{
			"keyset":   keyset,
			"keyspace": keyspace,
			"type":     "number",
		},
		float64(numBytes),
	)

	if err = iter.Close(); err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsFunc, "getTST").Err(err).Send()
		}

		if err == gocql.ErrNotFound {
			return map[string][]TextPnt{}, 0, errNoContent("getTST")
		}

		persist.statsSelectFerror(keyspace, "ts_text_stamp")
		return map[string][]TextPnt{}, 0, errPersist("getTST", err)
	}

	persist.statsSelect(keyspace, "ts_text_stamp", time.Since(track), countRows)

	if limitReached && !allowFullFetch {
		return map[string][]TextPnt{}, numBytes, errMaxBytesLimitWrapper("GetTS", persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}
