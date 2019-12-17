package plot

import (
	"fmt"
	"time"

	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
)

func (persist *persistence) GetTS(keyspace string, keys []string, start, end int64, ms, allowFullFetch bool, maxBytesLimit uint32, keyset string) (map[string][]Pnt, uint32, gobol.Error) {

	track := time.Now()
	start--
	end++

	var tsid string
	var date int64
	var value float64
	var err error
	var numBytes uint32
	idsGroup := persist.buildInGroup(keys)

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT id, date, value FROM %v.ts_number_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`,
			keyspace,
			idsGroup,
		),
		start,
		end,
	).Iter()

	tsMap := map[string][]Pnt{}
	countRows := 0
	limitReached := false

	for iter.Scan(&tsid, &date, &value) {

		if !ms {
			date = (date / 1000) * 1000
		}

		point := Pnt{
			Date:  date,
			Value: value,
		}

		if _, ok := tsMap[tsid]; !ok {
			numBytes += uint32(persist.getStringSize(tsid))
		}

		tsMap[tsid] = append(tsMap[tsid], point)

		numBytes += uint32(persist.constPartBytesFromNumberPoint)

		if !allowFullFetch && numBytes >= maxBytesLimit {
			limitReached = true
			break
		}

		countRows++
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
			logh.Error().Str(constants.StringsFunc, "getTS").Err(err).Send()
		}

		if err == gocql.ErrNotFound {
			persist.statsSelect(keyspace, "ts_number_stamp", time.Since(track), countRows)
			return map[string][]Pnt{}, 0, errNoContent("getTS")
		}

		persist.statsSelectQerror(keyspace, "ts_number_stamp")
		return map[string][]Pnt{}, 0, errPersist("getTS", err)
	}

	persist.statsSelect(keyspace, "ts_number_stamp", time.Since(track), countRows)

	if limitReached && !allowFullFetch {
		return map[string][]Pnt{}, numBytes, errMaxBytesLimitWrapper("GetTS", persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}
