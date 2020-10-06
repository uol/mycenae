package plot

import (
	"fmt"
	"time"

	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
)

const (
	funcGetTS                 string = "GetTS"
	queryGetTS                string = `SELECT id, date, value FROM %s.ts_number_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`
	funcGetLastTS             string = "GetLastTS"
	queryGetLastTSNoTimestamp string = `SELECT id, date, value FROM %s.ts_number_stamp WHERE id = ? limit 1`              // given that clustering order MUST be date desc
	queryGetLastTS            string = `SELECT id, date, value FROM %s.ts_number_stamp WHERE id = ? AND date < ? limit 1` // given that clustering order MUST be date desc
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
	_, unlimitedBytes := persist.unlimitedBytesKeysetWhiteList[keyset]
	allowFullFetch = allowFullFetch || unlimitedBytes

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			queryGetTS,
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

		if _, ok := tsMap[tsid]; !ok {
			numBytes += uint32(persist.getStringSize(tsid))
		}

		if persist.clusteringOrder == constants.ClusteringOrderDESC {
			tsMap[tsid] = append(tsMap[tsid], Pnt{})
			copy(tsMap[tsid][1:], tsMap[tsid])
			tsMap[tsid][0].Date = date
			tsMap[tsid][0].Value = value
		} else {
			tsMap[tsid] = append(tsMap[tsid], Pnt{
				Date:  date,
				Value: value,
			})
		}

		numBytes += uint32(persist.constPartBytesFromNumberPoint)

		countRows++

		if !allowFullFetch && numBytes >= maxBytesLimit {
			limitReached = true
			break
		}

	}

	persist.statsQueryBytes(funcGetTS, keyset, keyspace, typeNumber, float64(numBytes))

	if err = iter.Close(); err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsFunc, funcGetTS).Err(err).Send()
		}

		if err == gocql.ErrNotFound {
			persist.statsSelect(funcGetTS, keyset, keyspace, typeNumber, time.Since(track), countRows)
			return map[string][]Pnt{}, 0, errNoContent(funcGetTS)
		}

		persist.statsQueryError(funcGetTS, keyset, keyspace, typeNumber)
		return map[string][]Pnt{}, 0, errPersist(funcGetTS, err)
	}

	persist.statsSelect(funcGetTS, keyset, keyspace, typeNumber, time.Since(track), countRows)

	if limitReached && !allowFullFetch {
		return map[string][]Pnt{}, numBytes, errMaxBytesLimitWrapper(funcGetTS, persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}

func (persist *persistence) GetLastTS(keyspace string, keys []string, end int64, ms, allowFullFetch bool, maxBytesLimit uint32, keyset string) (map[string]Pnt, uint32, gobol.Error) {

	var tsid string
	var date int64
	var value float64
	var err error
	var numBytes uint32
	_, unlimitedBytes := persist.unlimitedBytesKeysetWhiteList[keyset]
	allowFullFetch = allowFullFetch || unlimitedBytes
	limitReached := false
	tsMap := map[string]Pnt{}
	countRows := 0

mainLoop:
	for _, id := range keys {

		track := time.Now()

		var iter *gocql.Iter
		if end == 0 {
			iter = persist.cassandra.Query(
				fmt.Sprintf(queryGetLastTSNoTimestamp, keyspace),
				id).
				Iter()
		} else {
			iter = persist.cassandra.Query(
				fmt.Sprintf(queryGetLastTS, keyspace),
				id,
				end).
				Iter()
		}

		if iter.Scan(&tsid, &date, &value) {

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

			tsMap[tsid] = point

			numBytes += uint32(persist.constPartBytesFromNumberPoint)

			countRows++

			if !allowFullFetch && numBytes >= maxBytesLimit {
				limitReached = true
				break mainLoop
			}
		}

		if err = iter.Close(); err != nil {

			if err == gocql.ErrNotFound {
				continue mainLoop
			}

			if logh.ErrorEnabled {
				logh.Error().Str(constants.StringsFunc, funcGetLastTS).Err(err).Send()
			}

			persist.statsQueryError(funcGetLastTS, keyset, keyspace, typeNumber)
			return map[string]Pnt{}, 0, errPersist(funcGetLastTS, err)
		}

		persist.statsSelect(funcGetLastTS, keyset, keyspace, typeNumber, time.Since(track), countRows)
	}

	persist.statsQueryBytes(funcGetLastTS, keyset, keyspace, typeNumber, float64(numBytes))

	if limitReached && !allowFullFetch {
		return map[string]Pnt{}, numBytes, errMaxBytesLimitWrapper(funcGetLastTS, persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}
