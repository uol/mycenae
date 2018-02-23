package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func sendPointsExpression(msg string, points interface{}) {

	jsonPoints, err := json.Marshal(points)
	if err != nil {
		log.Fatal(msg, err, jsonPoints)
	}

	code, resp, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code != http.StatusNoContent {
		log.Fatal(msg, " sendPointsExpression ", code, " resp ", string(resp))
	}

	time.Sleep(3 * time.Second)
}

/*
TS01
    serie: 0,1,2,3,4...
    interval: 1m
    total: 100 points
*/
func ts1TsdbExpression(startTime int) (string, string) {
	//startTime := 1448452800
	ts01tsdbexpression := fmt.Sprintf("ts01tsdb.expression-%d-%d", rand.Int(), startTime)
	value := 0.0
	const numTotal int = 100
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts01tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		value++
		startTime += 60
	}

	sendPointsExpression("ts1TsdbExpression", Points)

	ts1IDTsdbExpression := tools.GetHashFromMetricAndTags(ts01tsdbexpression, map[string]string{"host": "test", "ksid": ksMycenae, "ttl": "1"})

	return ts01tsdbexpression, ts1IDTsdbExpression
}

/*
TS01_1
    serie: 0,1,2,3,4...
    interval: 1m
    total: 100 points
*/
func ts1_1TsdbExpression(startTime int) (string, string, string) {
	//startTime := 1448452800

	dateStart := startTime
	ts01_1tsdbexpression := fmt.Sprint("ts01_1tsdb.expression-", startTime)
	value := 0.0
	const numTotal int = 100
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts01_1tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(dateStart)
		value++
		dateStart += 60
	}

	sendPointsExpression("ts1_1TsdbExpression", Points)

	ts1_1IDTsdbExpression := tools.GetHashFromMetricAndTags(ts01_1tsdbexpression, map[string]string{"host": "test", "ksid": ksMycenae, "ttl": "1"})

	//startTime = 1448452800
	dateStart = startTime
	value = 0.0
	Points = [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts01_1tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test2",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(dateStart)
		value++
		dateStart += 60
	}

	sendPointsExpression("ts1_1TsdbExpression", Points)

	ts1_1IDTsdbExpression2 := tools.GetHashFromMetricAndTags(ts01_1tsdbexpression, map[string]string{"host": "test2", "ksid": ksMycenae, "ttl": "1"})

	return ts01_1tsdbexpression, ts1_1IDTsdbExpression, ts1_1IDTsdbExpression2
}

/*
TS02
    serie: 0,1,2,1,2,3,2,3,4...
    interval: 1m
    total: 90 points
*/
func ts2TsdbExpression(startTime int) (string, string) {
	value := 0.0
	const numTotal int = 90
	ts02tsdbexpression := fmt.Sprintf("ts02tsdb.expression-%d-%d", rand.Int(), startTime)
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts02tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		i++
		Points[i].Value = float32(value + 1.0)
		Points[i].Metric = ts02tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		i++
		startTime += 60
		Points[i].Value = float32(value + 2.0)
		Points[i].Metric = ts02tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		value++
		startTime += 60
	}

	sendPointsExpression("ts2TsdbExpression", Points)

	ts2IDTsdbExpression := tools.GetHashFromMetricAndTags(ts02tsdbexpression, map[string]string{"host": "test", "ksid": ksMycenae, "ttl": "1"})

	return ts02tsdbexpression, ts2IDTsdbExpression
}

/*
TS03
    serie: 0,5,10,15,20...
    interval: 30m
    total: 480 points
*/
func ts3TsdbExpression(startTime int) (string, string) {
	//startTime := 1448452800
	ts03tsdbexpression := fmt.Sprint("ts03tsdb.expression-", startTime)
	value := 0.0

	const numTotal int = 480
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts03tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		value += 5
		startTime += 60 * 30
	}

	sendPointsExpression("ts3TsdbExpression", Points)

	ts3IDTsdbExpression := tools.GetHashFromMetricAndTags(ts03tsdbexpression, map[string]string{"host": "test", "ksid": ksMycenae, "ttl": "1"})

	return ts03tsdbexpression, ts3IDTsdbExpression
}

/*
TS04
    serie: 0.5,1.0,1.5,2.0...
    interval: 1 week
    total: 208 points
*/
func ts4TsdbExpression(startTime int) (string, string) {
	//startTime := 1448452800
	ts04tsdbexpression := fmt.Sprint("ts04tsdb.expression-", startTime)
	value := 0.0

	const numTotal int = 208
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = ts04tsdbexpression
		Points[i].Tags = map[string]string{
			"ksid": ksMycenae,
			"host": "test",
			"ttl":  "1",
		}
		Points[i].Timestamp = int64(startTime)
		value += 0.5
		startTime += 604800
	}

	sendPointsExpression("ts4TsdbExpression", Points)

	ts4IDTsdbExpression := tools.GetHashFromMetricAndTags(ts04tsdbexpression, map[string]string{"host": "test", "ksid": ksMycenae, "ttl": "1"})

	return ts04tsdbexpression, ts4IDTsdbExpression
}

func postExpressionAndCheck(t *testing.T, expression, metric string, p, dps, tags, aggtags, tsuuidSize int) ([]tools.ResponseQuery, []string) {

	path := fmt.Sprintf("keysets/%s/query/expression?tsuid=true&exp=%s", ksMycenae, expression)
	code, response, err := mycenaeTools.HTTP.GET(path)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	queryPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &queryPoints)
	if err != nil {
		t.Error(err, string(response))
		t.SkipNow()
	}

	keys := []string{}

	for key := range queryPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, metric, queryPoints[0].Metric)
	assert.Equal(t, p, len(queryPoints))
	assert.Equal(t, dps, len(queryPoints[0].Dps))
	assert.Equal(t, tags, len(queryPoints[0].Tags))
	assert.Equal(t, aggtags, len(queryPoints[0].AggTags))
	assert.Equal(t, tsuuidSize, len(queryPoints[0].Tsuuids))

	return queryPoints, keys
}

func TestTsdbExpressionFilterPeriodMin(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Unix())

	metric, tsid := ts1TsdbExpression(startTime - 5940)

	expression := fmt.Sprintf(`merge(sum,query(%v,{host=test},10m))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 10, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 90.0
	startTime -= 540

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 60
		i++
	}

}

func TestTsdbExpressionFilterPeriodSec(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Unix())

	metric, tsid := ts1TsdbExpression(startTime - 5940)

	expression := fmt.Sprintf(`merge(sum,query(%v,{host=test},599s))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 10, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 90.0
	startTime -= 540

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 60
		i++
	}

}

func TestTsdbExpressionFilterPeriodMs(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Unix())

	metric, tsid := ts1TsdbExpression(startTime - 5940)

	expression := fmt.Sprintf(`merge(sum,query(%v,{host=test},600000ms))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 10, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 90.0
	startTime -= 540

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 60
		i++
	}

}

func TestTsdbExpressionFilterDownsampleAvg(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Minute).Unix()) - 5400 // 90m ago

	metric, tsid := ts2TsdbExpression(startTime)

	offset := "90m"
	// if the test is run during a minute change,
	// increment a minute in the relative time (offset) to get the first point
	if int(time.Now().Truncate(time.Minute).Unix()-5400)-startTime == 60 {
		offset = "91m"
	}

	expression := fmt.Sprintf(`merge(avg, downsample(3m, avg, none, query(%v,{host=test},%s)))`, metric, offset)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 30, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 0
	for _, key := range keys {

		if i == 0 {
			assert.Exactly(t, 1.5, queryPoints[0].Dps[key])
		} else {
			media := float64((i + i + 1 + i + 2) / 3)
			assert.Exactly(t, media, queryPoints[0].Dps[key])
		}

		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 180
		i++
	}

}

func TestTsdbExpressionFilterDownsampleMin(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Minute).Unix()) - 5400

	metric, tsid := ts2TsdbExpression(startTime)

	offset := "90m"
	// if the test is run during a minute change,
	// increment a minute in the relative time (offset) to get the first point
	if int(time.Now().Truncate(time.Minute).Unix()-5400)-startTime == 60 {
		offset = "91m"
	}

	expression := fmt.Sprintf(`merge(min, downsample(3m, min, none, query(%v,{host=test},%s)))`, metric, offset)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 30, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])
	i := 0.0

	for _, key := range keys {

		if i == 0 {
			assert.Exactly(t, 1.0, queryPoints[0].Dps[key])
		} else {
			assert.Exactly(t, i, queryPoints[0].Dps[key])
		}

		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 180
		i++
	}
}

func TestTsdbExpressionFilterDownsampleMax(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Minute).Unix()) - 5400

	metric, tsid := ts2TsdbExpression(startTime)

	offset := "90m"
	// if the test is run during a minute change,
	// increment a minute in the relative time (offset) to get the first point
	if int(time.Now().Truncate(time.Minute).Unix()-5400)-startTime == 60 {
		offset = "91m"
	}

	expression := fmt.Sprintf(`merge(max, downsample(3m, max, none, query(%v,{host=test},%s)))`, metric, offset)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 30, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 2.0

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 180
		i++
	}
}

func TestTsdbExpressionFilterDownsampleSum(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Minute).Unix()) - 5400

	metric, tsid := ts2TsdbExpression(startTime)

	//time.Sleep(time.Second)
	offset := "90m"
	// if the test is run during a minute change,
	// increment a minute in the relative time (offset) to get the first point
	if int(time.Now().Truncate(time.Minute).Unix()-5400)-startTime == 60 {
		offset = "91m"
	}

	expression := fmt.Sprintf(`merge(sum, downsample(3m, sum, none, query(%v,{host=test},%s)))`, metric, offset)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 30, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 0.0

	for _, key := range keys {

		sum := float64(i + i + 1 + i + 2)
		assert.Exactly(t, sum, queryPoints[0].Dps[key])

		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 180
		i++
	}
}

func TestTsdbExpressionFilterDownsampleMaxHour(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Hour).Unix()) - 864000

	metric, tsid := ts3TsdbExpression(startTime)

	expression := fmt.Sprintf(`merge(max, downsample(2h, max, none, query(%v,{host=test},240h)))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 120, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 15.0
	//dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		i += 20.0

		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 7200
	}

}

func TestTsdbExpressionFilterDownsampleMaxDay(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Hour*24).Unix()) - 864000

	metric, tsid := ts3TsdbExpression(startTime)

	expression := fmt.Sprintf(`merge(max, downsample(2d, max, none, query(%v,{host=test},10d)))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 5, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 475.0
	//fmt.Println(int(time.Random().Unix()))
	//fmt.Println(startTime)
	//fmt.Println((int(time.Random().Unix()))-startTime)

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key])
		i += 480.0

		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 172800
	}
}

func TestTsdbExpressionFilterDownsampleMaxWeek(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Truncate(time.Hour*24*7).Unix()) - 125193600

	metric, tsid := ts4TsdbExpression(startTime)

	expression := fmt.Sprintf(`merge(max, downsample(4w, max, none, query(%v,{host=test},207w)))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 52, 3, 0, 1)

	assert.Equal(t, "test", queryPoints[0].Tags["host"])
	assert.Equal(t, tsid, queryPoints[0].Tsuuids[0])

	i := 1.5
	//fmt.Println(int(time.Random().Unix()))
	//fmt.Println(startTime)
	//fmt.Println((int(time.Random().Unix()))-startTime)

	for _, key := range keys {

		assert.Exactly(t, i, queryPoints[0].Dps[key], expression)
		i += 2.0
		assert.Exactly(t, strconv.Itoa(startTime), key, expression)
		//+4weeks
		startTime += 2419200
	}
}

func TestTsdbExpressionFilterRateTrueNoPoints(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Unix())

	metric, _ := ts1TsdbExpression(startTime)

	expression := url.QueryEscape(fmt.Sprintf(`rate(true, null, 0, merge(sum, query(%v,{host=test1},1s)))`, metric))

	code, response, _ := mycenaeTools.HTTP.GET("keysets/" + ksMycenae + "/query/expression?tsuid=true&exp=" + expression)

	assert.Equal(t, 200, code)
	assert.Equal(t, "[]", string(response))

}

func TestTsdbExpressionMergeDateLimit(t *testing.T) {
	t.Parallel()
	startTime := int(time.Now().Unix())

	metric, tsid1, tsid2 := ts1_1TsdbExpression(startTime - 5940)

	expression := fmt.Sprintf(`merge(sum, query(%v,null,10m))`, metric)
	queryPoints, keys := postExpressionAndCheck(t, url.QueryEscape(expression), metric, 1, 10, 2, 1, 2)

	assert.Equal(t, "host", queryPoints[0].AggTags[0])
	assert.Contains(t, queryPoints[0].Tsuuids, tsid1, "Tsuuid not found")
	assert.Contains(t, queryPoints[0].Tsuuids, tsid2, "Tsuuid not found")

	i := 90.0
	//calculate the time difference for first point
	startTime -= 540

	for _, key := range keys {

		assert.Exactly(t, i+i, queryPoints[0].Dps[key])
		assert.Exactly(t, strconv.Itoa(startTime), key)
		startTime += 60
		i++
	}
}

func TestTsdbExpressionError(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		query    string
		errorMsg string
	}{
		"InvalidFunction": {
			`groupBy({host=*})|test(sum, query(metric,null,1y))`,
			"unkown function test",
		},
		"EmptyFunction": {
			`merge(sum, (metric,null,1y))`,
			"unkown function ",
		},
		"InvalidRateOptionCounter": {
			`rate(a, null, 0, merge(sum,query(metric,null,1y)))`,
			"strconv.ParseBool: parsing \"a\": invalid syntax",
		},
		"InvalidRateOptionCounterMax": {
			`rate(true, "a", 0, merge(sum,query(metric,null,1y)))`,
			"strconv.ParseInt: parsing \"\\\"a\\\"\": invalid syntax",
		},
		"InvalidRateOptionCounterMaxNegative": {
			`rate(true, -1, 0, merge(sum,query(metric,null,1y)))`,
			"counter max needs to be a positive integer",
		},
		"InvalidRateOptionResetValue": {
			`rate(true, null, a, merge(sum,query(metric,null,1y)))`,
			"strconv.ParseInt: parsing \"a\": invalid syntax",
		},
	}

	for test, data := range cases {

		path := fmt.Sprintf("keysets/%s/query/expression?tsuid=true&exp=%s", ksMycenae, url.QueryEscape(data.query))
		code, response, err := mycenaeTools.HTTP.GET(path)
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		queryError := tools.Error{}

		err = json.Unmarshal(response, &queryError)
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		assert.Equal(t, 400, code, test)
		assert.Equal(t, data.errorMsg, queryError.Error, test)
	}
}
