package plot

import (
	"fmt"
	"regexp"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
)

const (
	funcGetTST      string = "GetTST"
	queryGetTST     string = `SELECT id, date, value FROM %s.ts_text_stamp WHERE id in (%s) AND date > ? AND date < ? ALLOW FILTERING`
	funcGetLastTST  string = "GetLastTST"
	queryGetLastTST string = `SELECT id, date, value FROM %s.ts_text_stamp where id = ? order by date desc limit 1`
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
	_, unlimitedBytes := persist.unlimitedBytesKeysetWhiteList[keyset]
	allowFullFetch = allowFullFetch || unlimitedBytes

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			queryGetTST,
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

	persist.statsQueryBytes(funcGetTST, keyset, keyspace, typeText, float64(numBytes))

	if err = iter.Close(); err != nil {
		if logh.ErrorEnabled {
			logh.Error().Str(constants.StringsFunc, funcGetTST).Err(err).Send()
		}

		if err == gocql.ErrNotFound {
			persist.statsSelect(funcGetTST, keyset, keyspace, typeText, time.Since(track), countRows)
			return map[string][]TextPnt{}, 0, errNoContent(funcGetTST)
		}

		persist.statsQueryError(funcGetTST, keyset, keyspace, typeText)
		return map[string][]TextPnt{}, 0, errPersist(funcGetTST, err)
	}

	persist.statsSelect(funcGetTST, keyset, keyspace, typeText, time.Since(track), countRows)

	if limitReached && !allowFullFetch {
		return map[string][]TextPnt{}, numBytes, errMaxBytesLimitWrapper(funcGetTST, persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}

func (persist *persistence) GetLastTST(keyspace string, keys []string, search *regexp.Regexp, allowFullFetch bool, maxBytesLimit uint32, keyset string) (map[string]TextPnt, uint32, gobol.Error) {

	var tsid string
	var date int64
	var value string
	var err error
	var numBytes uint32
	_, unlimitedBytes := persist.unlimitedBytesKeysetWhiteList[keyset]
	allowFullFetch = allowFullFetch || unlimitedBytes
	limitReached := false
	tsMap := map[string]TextPnt{}
	countRows := 0

mainLoop:
	for _, id := range keys {

		track := time.Now()

		iter := persist.cassandra.Query(
			fmt.Sprintf(
				queryGetLastTST,
				keyspace,
			),
			id,
		).Iter()

		if iter.Scan(&tsid, &date, &value) {

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

				tsMap[tsid] = point

				numBytes += uint32(persist.constPartBytesFromTextPoint + persist.getStringSize(value))

				countRows++

				if !allowFullFetch && numBytes >= maxBytesLimit {
					limitReached = true
					break mainLoop
				}
			}
		}

		if err = iter.Close(); err != nil {

			if err == gocql.ErrNotFound {
				continue mainLoop
			}

			if logh.ErrorEnabled {
				logh.Error().Str(constants.StringsFunc, funcGetLastTST).Err(err).Send()
			}

			persist.statsQueryError(funcGetLastTST, keyset, keyspace, typeNumber)
			return map[string]TextPnt{}, 0, errPersist(funcGetLastTST, err)
		}

		persist.statsSelect(funcGetLastTST, keyset, keyspace, typeNumber, time.Since(track), countRows)
	}

	persist.statsQueryBytes(funcGetLastTST, keyset, keyspace, typeNumber, float64(numBytes))

	if limitReached && !allowFullFetch {
		return map[string]TextPnt{}, numBytes, errMaxBytesLimitWrapper(funcGetLastTST, persist.maxBytesErr)
	}

	return tsMap, numBytes, nil
}
