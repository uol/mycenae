package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var (
	ttlKeyspaceKeySet            string
	ttlTSUIDMap, ttlTSUIDTextMap map[uint8]node
	startTime, endTime           time.Time
	countRegex                   *regexp.Regexp
)

const (
	NUMBER_METRIC  = "ttl_keyspace"
	TEXT_METRIC    = "ttl_keyspace_txt"
	HOST_TAG_KEY   = "host"
	HOST_TAG_VALUE = "test-host"
)

type node struct {
	totalPoints int
	ids         []string
	payloads    []tools.Payload
}

type getCount func(t *testing.T, ttl uint8, id string) int

func mapPoints(ttl uint8, idMap map[uint8]node, payloads []tools.Payload, ids []string) {

	m := map[string]bool{}
	uniqueIds := []string{}
	for _, id := range ids {
		if !m[id] {
			m[id] = true
			uniqueIds = append(uniqueIds, id)
		}
	}

	idMap[ttl] = node{
		totalPoints: len(payloads),
		ids:         uniqueIds,
		payloads:    payloads,
	}
}

func sendPointsToTTLKeyspace(keySet string) {

	fmt.Println("Setting up ttlKeyspace_test.go tests...")

	countRegex = regexp.MustCompile("\"count\":([0-9]+)")
	ttlKeyspaceKeySet = keySet
	ttlTSUIDMap = map[uint8]node{}
	ttlTSUIDTextMap = map[uint8]node{}

	startTime = time.Now()
	currentTime := startTime

	for ttl := range tools.TTLKeyspaceMap {

		ps, ids, updatedTime := sendRandomPoints(int(ttl), ttl, NUMBER_METRIC, true, currentTime)
		mapPoints(ttl, ttlTSUIDMap, ps, ids)

		ps, ids, updatedTime = sendRandomPoints(int(ttl), ttl, TEXT_METRIC, false, updatedTime)
		mapPoints(ttl, ttlTSUIDTextMap, ps, ids)

		currentTime = updatedTime
	}

	endTime = currentTime
}

func sendRandomPoints(num int, ttl uint8, metric string, isNumber bool, lastSentPoint time.Time) ([]tools.Payload, []string, time.Time) {

	ps := []tools.Payload{}
	tsuids := []string{}

	for i := 0; i < num; i++ {

		lastSentPoint = lastSentPoint.Add(1 * time.Second)

		value := rand.Intn(100)
		tags := map[string]string{
			"ttl":        strconv.Itoa(int(ttl)),
			"ksid":       ttlKeyspaceKeySet,
			HOST_TAG_KEY: HOST_TAG_VALUE,
		}

		var p tools.Payload

		if isNumber {
			p = tools.CreatePayloadTS(float32(value), metric, tags, lastSentPoint.Unix())
		} else {
			p = tools.CreateTextPayloadTS("text-"+strconv.Itoa(value), metric, tags, lastSentPoint.Unix())
		}

		ps = append(ps, p)
		tsuids = append(tsuids, tools.GetTSUIDFromPayload(&p, isNumber))
	}

	var api string

	if isNumber {
		api = "api/put"
	} else {
		api = "api/text/put"
	}

	jsonBytes, err := json.Marshal(ps)

	if err != nil {
		panic(err)
	}

	code, resp, _ := mycenaeTools.HTTP.POST(api, jsonBytes)

	if code != http.StatusNoContent {
		log.Fatal("error sending points, code: ", code, " ttlKeyspace_test.go, response: ", string(resp))
	}

	return ps, tsuids, lastSentPoint
}

func getTSUID(year, week int, id string) string {
	return fmt.Sprintf("%v%v%v", year, week, id)
}

func runTest(t *testing.T, f getCount, m map[uint8]node, pointType string) {

	year, week := time.Now().ISOWeek()

	for ttl, node := range m {

		count := 0
		for _, id := range node.ids {
			tsuid := getTSUID(year, week, id)
			count += f(t, ttl, tsuid)
			assert.True(t, count > 0, "no %s points found for ttl %d and id %s", pointType, ttl, tsuid)
		}

		assert.True(t, count == node.totalPoints, "missing %s points for ttl: %d (%d != %d)", pointType, ttl, count, node.totalPoints)
	}
}

func getCountFromScylla(t *testing.T, ttl uint8, id string) int {
	return mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(int(ttl), id)
}

func getTextCountFromScylla(t *testing.T, ttl uint8, id string) int {
	return mycenaeTools.Cassandra.Timeseries.CountTextFromIDSTAMP(int(ttl), id)
}

func TestTTLKeyspaceCheckPointsInScylla(t *testing.T) {

	t.Parallel()

	runTest(t, getCountFromScylla, ttlTSUIDMap, "number")
	runTest(t, getTextCountFromScylla, ttlTSUIDTextMap, "text")
}

func queryByTTL(t *testing.T, ttl uint8, id string, isNumber bool) int {

	runes := []rune(id)

	runeIndex := 5
	if _, w := startTime.ISOWeek(); w >= 10 {
		runeIndex = 6
	}

	tsid := string(runes[runeIndex:])

	tpl := `{
		"%s": [{
			"tsid":"%s",
			"ttl": %d
		}],
		"start": %d,
		"end": %d
	}`

	var qType string
	if isNumber {
		qType = "keys"
	} else {
		qType = "text"
	}

	payload := fmt.Sprintf(tpl,
		qType,
		tsid,
		int(ttl),
		startTime.Unix()*1000,
		endTime.Unix()*1000)

	code, response, err := mycenaeTools.HTTP.POST("keysets/"+ttlKeyspaceKeySet+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
		t.Log(payload)
	}

	assert.Equal(t, http.StatusOK, code)

	items := countRegex.FindAllStringSubmatch(string(response), -1)

	if items != nil {
		count, err := strconv.Atoi(items[0][1])
		if err != nil {
			t.Error(err)
			t.SkipNow()
		}
		return count
	} else {
		t.Error("no 'count' attribute was found on response: ", string(response))
		t.SkipNow()
		return 0
	}
}

func getCountUsingAPI(t *testing.T, ttl uint8, id string) int {
	return queryByTTL(t, ttl, id, true)
}

func getCountTextUsingAPI(t *testing.T, ttl uint8, id string) int {
	return queryByTTL(t, ttl, id, false)
}

func TestTTLKeyspaceCheckPointsUsingV2Query(t *testing.T) {

	t.Parallel()

	runTest(t, getCountUsingAPI, ttlTSUIDMap, "number")
	runTest(t, getCountTextUsingAPI, ttlTSUIDTextMap, "text")
}

func checkMetadata(t *testing.T, uri string) {

	body := `{
		"metric":".*"
    }`

	code, response := requestResponse(t, uri, body)

	assert.Equal(t, 200, code)
	assert.Equal(t, len(tools.TTLKeyspaceMap), response.TotalRecord)

	payloads := response.Payload

	assert.True(t, len(payloads) == len(tools.TTLKeyspaceMap), "wrong number of ttl keyspace metas found: expected %d, found %d", len(tools.TTLKeyspaceMap), len(payloads))

	ttlTagMap := map[uint8]bool{}
	for ttl, _ := range tools.TTLKeyspaceMap {
		ttlTagMap[ttl] = true
	}

	for _, payload := range payloads {
		ttlVal, err := strconv.ParseInt(payload.Tags["ttl"], 10, 8)
		if err != nil {
			t.Error(err)
			t.SkipNow()
			continue
		}
		ttl := uint8(ttlVal)
		assert.True(t, ttlTagMap[ttl], "expected ttl %d was not found", ttl)
		delete(ttlTagMap, ttl)

		assert.True(t, payload.Metric == NUMBER_METRIC || payload.Metric == TEXT_METRIC, "unexpected metric %s found", payload.Metric)

		if value, ok := payload.Tags[HOST_TAG_KEY]; !ok {
			assert.Fail(t, "tag key %s was not found", HOST_TAG_KEY)
		} else {
			assert.True(t, value == HOST_TAG_VALUE, "unexpected value %s found", value)
		}
	}
}

func TestTTLKeyspaceMetadata(t *testing.T) {

	t.Parallel()

	checkMetadata(t, fmt.Sprintf("keysets/%s/meta", ttlKeyspaceKeySet))
	checkMetadata(t, fmt.Sprintf("keysets/%s/text/meta", ttlKeyspaceKeySet))
}
