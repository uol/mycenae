package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	//"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/tests/tools"
	"math"
)

var ts10IDTsdbQuery, ts12IDTsdbQuery, ts13IDTsdbQuery, ts13IDTsdbQuery2, ts13IDTsdbQuery3,
	ts13IDTsdbQuery4, ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8 string

var hashMap map[string]string

// Helper

func postAPIQueryAndCheck(t *testing.T, payload string, metric string, p, dps, tags, aggtags, tsuuidSize int, tsuuids ...string) ([]string, []tools.ResponseQuery) {

	path := fmt.Sprintf("keyspaces/%s/api/query", ksMycenae)
	code, response, err := mycenaeTools.HTTP.POST(path, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err, string(response))
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, metric, payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, p, len(payloadPoints))
	assert.Equal(t, dps, len(payloadPoints[0].Dps))
	assert.Equal(t, tags, len(payloadPoints[0].Tags))
	assert.Equal(t, aggtags, len(payloadPoints[0].AggTags))
	assert.Equal(t, tsuuidSize, len(payloadPoints[0].Tsuuids))

	for _, name := range tsuuids {
		assert.Contains(t, payloadPoints[0].Tsuuids, hashMap[name], "Tsuuid not found")
	}

	return keys, payloadPoints
}

func sendPointsPointsGrafana(keyspace string) {

	fmt.Println("Setting up pointsGrafana_test.go tests...")

	tsInsert(keyspace)
	ts10TsdbQuery(keyspace)
	ts12TsdbQuery(keyspace)
	ts13TsdbQuery(keyspace)
	ts16TsdbQuery(keyspace)
}

func round(val float64, roundOn float64, places int) float64 {

	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)

	var round float64
	if val > 0 {
		if div >= roundOn {
			round = math.Ceil(digit)
		} else {
			round = math.Floor(digit)
		}
	} else {
		if div >= roundOn {
			round = math.Floor(digit)
		} else {
			round = math.Ceil(digit)
		}
	}

	return round / pow
}

func roundValue(price float64) float64 {
	return round(price, 0.5, 4)
}

//func testSubQuery(t *testing.T, expected, actual structs.TSDBquery) {
//
//	assert.Equal(t, expected.Aggregator, actual.Aggregator)
//	assert.Equal(t, expected.Metric, actual.Metric)
//	assert.Equal(t, expected.TSUIDs, actual.TSUIDs)
//
//	if expected.Downsample != nil && assert.NotNil(t, actual.Downsample) {
//		assert.Equal(t, *expected.Downsample, *actual.Downsample)
//	}
//
//	assert.Equal(t, expected.Rate, actual.Rate)
//	if expected.Rate {
//		assert.Equal(t, expected.RateOptions.Counter, actual.RateOptions.Counter)
//		assert.Equal(t, expected.RateOptions.ResetValue, actual.RateOptions.ResetValue)
//
//		if expected.RateOptions.CounterMax != nil && assert.NotNil(t, actual.RateOptions.CounterMax) {
//			assert.Equal(t, *expected.RateOptions.CounterMax, *actual.RateOptions.CounterMax)
//		}
//	}
//
//	assert.Equal(t, len(expected.Filters), len(actual.Filters))
//	for i, f := range expected.Filters {
//		assert.Equal(t, f.Filter, actual.Filters[i].Filter)
//		assert.Equal(t, f.Ftype, actual.Filters[i].Ftype)
//		assert.Equal(t, f.GroupByResp, actual.Filters[i].GroupByResp)
//		assert.Equal(t, f.Tagk, actual.Filters[i].Tagk)
//	}
//
//	if expected.Index != nil && assert.NotNil(t, actual.Index) {
//		assert.Equal(t, *expected.Index, *actual.Index)
//	}
//
//	assert.Equal(t, expected.Tags, actual.Tags)
//	assert.Equal(t, expected.FilterTagKs, actual.FilterTagKs)
//	assert.Equal(t, expected.ExplicitTags, actual.ExplicitTags)
//
//}

func tsInsert(keyspace string) {

	hashMap = map[string]string{}
	cases := map[string]struct {
		metric     string
		tagValue   string
		startTime  int
		iStartTime int
		value      float64
		iValue     float64
		numTotal   int
	}{
		// serie: 0,1,2,3,4...
		"ts1TsdbQuery":    {"ts01tsdb", "test", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_1TsdbQuery":  {"ts01_1tsdb", "test", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_1TsdbQuery2": {"ts01_1tsdb", "test2", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_2TsdbQuery":  {"ts01_2tsdb", "test1", 1448452830, 60, 0.0, 1.0, 100},
		"ts1_2TsdbQuery2": {"ts01_2tsdb", "test2", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_3TsdbQuery":  {"ts01_3tsdb", "test.1", 1348452830, 60, 0.0, 1.0, 100},
		"ts1_3TsdbQuery2": {"ts01_3tsdb", "test.2", 1348452800, 60, 0.0, 1.0, 100},
		// serie: 0,1,2,1,2,3,2,3,4...
		"ts2TsdbQuery":  {"ts02tsdb", "test", 1448452800, 180, 0.0, 1.0, 30},
		"ts2TsdbQuery2": {"ts02tsdb", "test", 1448452860, 180, 1.0, 1.0, 30},
		"ts2TsdbQuery3": {"ts02tsdb", "test", 1448452920, 180, 2.0, 1.0, 30},
		// serie: 0,5,10,15,20...
		"ts3TsdbQuery": {"ts03tsdb", "test", 1448452800, 1800, 0.0, 5, 480},
		// serie: 0.5,1.0,1.5,2.0...
		"ts4TsdbQuery": {"ts04tsdb", "test", 1448452800, 604800, 0.0, 0.5, 208},
		// serie: 0, 2, 4, 6, 8...
		"ts5TsdbQuery":  {"ts05tsdb", "test1", 1448452800, 120, 0.0, 2.0, 45},
		"ts5TsdbQuery2": {"ts05tsdb", "test2", 1448452860, 120, 1.0, 2.0, 45},
		"ts5TsdbQuery3": {"ts05tsdb", "test3", 1448452800, 60, 0.0, 1.0, 90},
		// serie: 1, 3, 5, 7, 9...
		"ts6TsdbQuery": {"ts06tsdb", "test1", 1448452860, 120, 1.0, 2.0, 45},
		// serie: 0, 1, 2, 3, 4...
		"ts7TsdbQuery":    {"ts07tsdb", "test1", 1448452800, 60, 0.0, 1.0, 90},
		"ts7TsdbQuery2":   {"ts07tsdb", "test2", 1448452800, 60, 0.0, 5.0, 90},
		"ts7_1TsdbQuery":  {"ts07_1tsdb", "test1", 1448452800, 60, 0.0, 1.0, 90},
		"ts7_1TsdbQuery2": {"ts07_1tsdb", "test2", 1448452800, 60, 0.0, 1.0, 15},
		"ts7_1TsdbQuery3": {"ts07_1tsdb", "test2", 1448457300, 60, 75, 1.0, 15},
		"ts7_2TsdbQuery":  {"ts07_2tsdb", "test1", 1448452800, 60, 0.0, 1.0, 90},
		"ts7_2TsdbQuery2": {"ts07_2tsdb", "test2", 1448452801, 60, 0.0, 5.0, 90},
		// serie: 0, 5, 10, 15, 20...
		"ts8TsdbQuery": {"ts08tsdb", "test", 1448452800, 60, 0.0, 5.0, 90},
		// serie: 0, 1, 2, 3,...,14,15,75,76,77,...,88,89
		"ts9TsdbQuery":    {"ts09tsdb", "test", 1448452800, 60, 0.0, 1.0, 15},
		"ts9TsdbQuery2":   {"ts09tsdb", "test", 1448457300, 60, 75, 1.0, 15},
		"ts9_1TsdbQuery":  {"ts09_1tsdb", "test", 1448452800, 60, 0.0, 1.0, 15},
		"ts9_1TsdbQuery2": {"ts09_1tsdb", "test", 1448457300, 60, 75, 1.0, 15},
		"ts9_1TsdbQuery3": {"ts09_1tsdb", "test2", 1448452800, 60, 0.0, 1.0, 15},
		"ts9_1TsdbQuery4": {"ts09_1tsdb", "test2", 1448457300, 60, 75, 1.0, 15},
		"ts11TsdbQuery":   {"ts11tsdb", "test3", 1448452800, 60, 0.0, 1.0, 25},
		// serie: 0, 1, 2, 3, 4...
		"ts14TsdbQuery":  {"ts14tsdb", "test1", 1448452800, 200, 0.0, 1.0, 90},
		"ts14TsdbQuery2": {"ts14tsdb", "test2", 1448452830, 200, 0.0, 1.0, 90},
		// serie: 0,1,2,3,4...
		"ts15TsdbQuery":   {"ts15tsdb", "test", 1451649702, 604800, 1.0, 1.0, 53},
		"ts17TsdbQuery":   {"ts17tsdb", "test", 1448452800, 60, 0.0, 1.0, 5},
		"ts17_1TsdbQuery": {"ts17tsdb", "test2", 1448452800, 60, 0.0, 1.0, 5},
	}

	for test, data := range cases {

		Points := make([]tools.Point, data.numTotal)

		for i := 0; i < data.numTotal; i++ {
			Points[i].Value = float32(data.value)
			Points[i].Metric = data.metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				"host": data.tagValue,
			}
			Points[i].Timestamp = int64(data.startTime)
			data.value += data.iValue
			data.startTime += data.iStartTime
		}

		sendPointsGrafana(test, Points)

		hashMap[test] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(data.metric, map[string]string{"host": data.tagValue})
	}
}

func sendPointsGrafana(msg string, points interface{}) {

	jsonPoints, err := json.Marshal(points)
	if err != nil {
		log.Fatal(msg, err, jsonPoints)
	}

	code, resp, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code != 204 {
		log.Fatal(msg, code, string(resp))
	}
}

func ts10TsdbQuery(keyspace string) {

	metric := "ts10tsdb"
	tagKey := "host"
	tagKey2 := "app"
	tagValue := "test2"
	tagValue2 := "test1"
	tagValue3 := "app1"
	startTime := 1448452800
	var value, value2 float32 = 0, 1
	const numTotal int = 50
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,
			tagKey: tagValue,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = float32(value2)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue2,
			tagKey2: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)

		value++
		value2 += 2
		startTime += 60
	}

	sendPointsGrafana("ts10tsdb:", Points)

	ts10IDTsdbQuery = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})

}

// serie: 1, 10, 100, 1000, 1000, 1,..,10000, 3000.
func ts12TsdbQuery(keyspace string) {

	metric := "ts12-_/.%&#;tsdb"
	tagKey := "hos-_/.%&#;t"
	tagValue := "test-_/.%&#;1"
	startTime := 1448452800
	value := 1.0
	const numTotal int = 12
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {

		if startTime < 1448453100 {
			Points[i].Value = float32(value)
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			value *= 10.0

		} else if startTime == 1448453100 {
			Points[i].Value = 1000.0
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			value = 1.0

		} else if startTime < 1448453460 {
			Points[i].Value = float32(value)
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			value *= 10.0

		} else {
			Points[i].Value = 3000.0
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			value = 1.0
		}

		startTime += 60
	}

	sendPointsGrafana("ts12tsdb:", Points)

	ts12IDTsdbQuery = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})

}

func ts13TsdbQuery(keyspace string) {

	metric := "ts13tsdb"
	tagKey := "host"
	tagKey2 := "app"
	tagKey3 := "type"
	tagValue := "host1"
	tagValue2 := "app1"
	tagValue3 := "type1"
	tagValue4 := "app2"
	startTime := 1448452800
	value := 1.0
	const numTotal int = 40
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = float32(value * 2)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++

	}

	sendPointsGrafana("ts13tsdb1:", Points)

	ts13IDTsdbQuery = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
	ts13IDTsdbQuery2 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})

	tagValue = "host2"
	tagValue3 = "type2"
	startTime = 1448452800
	value = 1.0
	Points = [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value * 3)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = float32(value * 4)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++
	}

	sendPointsGrafana("ts13tsdb2:", Points)

	ts13IDTsdbQuery3 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
	ts13IDTsdbQuery4 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})

	tagValue = "host3"
	tagValue3 = "type3"
	startTime = 1448452800
	value = 1.0
	Points = [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value * 5)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = float32(value * 6)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++
	}

	sendPointsGrafana("ts13tsdb3:", Points)

	ts13IDTsdbQuery5 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
	ts13IDTsdbQuery6 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})

	startTime = 1448452800
	value = 1.0

	tagValue3 = "type4"
	tagValue5 := "type5"
	Points = [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(value * 7)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = float32(value * 8)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey3: tagValue5,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++
	}

	sendPointsGrafana("ts13tsdb4:", Points)

	ts13IDTsdbQuery7 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey3: tagValue3})
	ts13IDTsdbQuery8 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey3: tagValue5})

}

func ts16TsdbQuery(keyspace string) {

	startTime := int64(1448452800)
	var start, end sync.WaitGroup

	start.Add(1)
	end.Add(9)
	for i := 0; i < 9; i++ {

		go func(i int, startTime int64) {

			start.Wait()
			sendPointsGrafana("ts16TsdbQuery", []tools.Point{
				{
					Value:  float32(i),
					Metric: "ts16tsdb",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime,
				},
				{
					Value:  float32(i + 9),
					Metric: "ts16tsdb",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime + 540,
				},
				{
					Value:  float32(i + 18),
					Metric: "ts16tsdb",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime + 1080,
				},
			})
			end.Done()

		}(i, startTime)

		startTime += 60
	}

	start.Done()
	end.Wait()

	hashMap["ts16TsdbQuery"] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags("ts16tsdb", map[string]string{"host": "test"})
}

// Concurrency

func TestTsdbQueryConcurrentPoints(t *testing.T) {

	dateStart := int64(1448452800000)

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts16tsdb",
    		"aggregator": "sum"
    	}]
	}`, dateStart, dateStart+1560000)

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts16tsdb", 1, 27, 1, 0, 1, "ts16TsdbQuery")

	for i, key := range keys {

		assert.Exactly(t, float32(i), float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 60000
	}
}

// Filter

func TestTsdbQueryFilter(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterFullYear(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1451649702000,
		"end": 1546257720000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts15tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts15tsdb", 1, 53, 1, 0, 1, "ts15TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 1.0
	dateStart := 1451649702
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 604800
		i++
	}
}

func TestTsdbQueryFilterNoTsuids(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": false,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 0)

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterMsResolution(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": true,
		"msResolution": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800000
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60000
		i++
	}
}

func TestTsdbQueryFilterDownsampleAvgExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "avg",
			"downsample": "3m-avg",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		media := float32((i + i + 1 + i + 2) / 3)
		assert.Exactly(t, media, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

/*
Failing Test
func TestTsdbQueryFilterDownsampleMsec(t *testing.T) {

	payload := `{
		"start": 1448452440000,
		"end": 1448458560001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "180000ms-min",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postApiQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}
*/

func TestTsdbQueryFilterDownsampleSec(t *testing.T) {

	payload := `{
		"start": 1448452440000,
		"end": 1448458560001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "180s-min",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryFilterDownsampleMin(t *testing.T) {

	payload := `{
		"start": 1448452440000,
		"end": 1448458560001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-min",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryFilterDownsampleMinExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-min",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryFilterDownsampleCountExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-count",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 3.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180

	}
}

func TestTsdbQueryFilterDownsampleCount(t *testing.T) {

	payload := `{
		"start": 1448452440000,
		"end": 1448458560001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-count",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 3.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryFilterDownsampleMaxExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "max",
			"downsample": "3m-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 2.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryFilterDownsampleSumExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "sum",
			"downsample": "3m-sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		sum := float32(i + i + 1 + i + 2)
		assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryFilterDownsampleCountSec(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "sum",
			"downsample": "30s-count-null",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, "ts02tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 180, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 0, len(payloadPoints[0].AggTags))
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids))

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])
	assert.Equal(t, hashMap["ts2TsdbQuery3"], payloadPoints[0].Tsuuids[0])

	dateStart := 1448452800
	count := 0
	for _, key := range keys {

		if count == 0 {
			assert.Exactly(t, 1.0, payloadPoints[0].Dps[key].(float64))
			count = 1
		} else {
			assert.Nil(t, payloadPoints[0].Dps[key])
			count = 0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryFilterDownsampleMaxHour(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1449316800000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts03tsdb",
			"aggregator": "max",
			"downsample": "2h-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts03tsdb", 1, 120, 1, 0, 1, "ts3TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 15.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 20.0

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 7200
	}
}

func TestTsdbQueryFilterDownsampleMaxDay(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1449316800000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts03tsdb",
			"aggregator": "max",
			"downsample": "2d-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts03tsdb", 1, 6, 1, 0, 1, "ts3TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 355.0
	dateStart := 1448409600

	for _, key := range keys {

		date, _ := strconv.Atoi(key)

		if date < 1449100800 {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 480.0

		} else {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 120.0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 172800
	}
}

func TestTsdbQueryFilterDownsampleMaxWeek(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1573646400000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "4w-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 52, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 1.5
	dateStart := time.Date(2015, 11, 23, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 2.0

		date := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(date), key)
		dateStart = dateStart.AddDate(0, 0, 28)
	}
}

func TestTsdbQueryFilterDownsampleMaxMonth(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1566734400000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "3n-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 16, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 4.5
	dateStart := time.Date(2015, 11, 1, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		date, _ := strconv.Atoi(key)

		if date <= 1501545600 {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 6.5

		} else if date == 1509494400 {

			i = 57.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1517443200 {

			i = 63.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1525132800 {

			i += 6.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1533081600 {

			i += 7.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date <= 1541030400 {

			i += 6.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1548979200 {

			i += 6.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1556668800 {

			i += 7.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			i += 1.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		}

		dateInt := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(dateInt), key)
		dateStart = dateStart.AddDate(0, 3, 0)
	}
}

func TestTsdbQueryFilterDownsampleMaxYear(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1545825600000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "1y-max",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 4, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 2.5
	dateStart := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 26.0

		date := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(date), key)
		dateStart = dateStart.AddDate(1, 0, 0)
	}
}

func TestTsdbQueryFilterMoreThanOneTS(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
		}]
		 },{
			"metric": "ts10tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test2",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 2, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}

	keys = []string{}

	for key := range payloadPoints[1].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 25, len(payloadPoints[1].Dps))
	assert.Equal(t, 1, len(payloadPoints[1].Tags))
	assert.Equal(t, 0, len(payloadPoints[1].AggTags))
	assert.Equal(t, 1, len(payloadPoints[1].Tsuuids))
	assert.Equal(t, "ts10tsdb", payloadPoints[1].Metric)
	assert.Equal(t, "test2", payloadPoints[1].Tags["host"])
	assert.Equal(t, ts10IDTsdbQuery, payloadPoints[1].Tsuuids[0])

	i = 0.0
	dateStart = 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[1].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterRegexpValidChars(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/\\.%\\&\\#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		i = i * 10.0
	}
}

func TestTsdbQueryFilterWildcardValidChars(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/.%&#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		i = i * 10.0
	}
}

func TestTsdbQueryFilterLiteralOrValidChars(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "literal_or",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/.%&#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		i = i * 10.0
	}
}

func TestTsdbQueryFilterNotLiteralOrValidChars(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "not_literal_or",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/.%&#;",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		i = i * 10.0
	}
}

func TestTsdbQueryFilterRateTrueRateOptionsFalse(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"rate": true,
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 99, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		calc := ((i + 1.0) - i) / float32((dateStart+60)-dateStart)
		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		dateStart += 60
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i++
	}
}

func TestTsdbQueryFilterRateTrueRateOptionsTrueCounter(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true
			},
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/\\.%\\&\\#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 4, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		dateStart += 60
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i = i * 10.0
	}
}

func TestTsdbQueryFilterRateTrueRateOptionsTrueCounterMax(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453100000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"counterMax": 15000
			},
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/\\.%\\&\\#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			calc := (countermax - i + 1000) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryFilterRateTrueRateOptionsTrueResetValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453100000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"resetValue": 10
			},
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/\\.%\\&\\#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			var calc float32
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryFilterRateTrueRateOptionsTrueCounterMaxAndResetValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453460000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"counterMax": 15000,
				"resetValue": 233
			},
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "hos-_/.%&#;t",
				"filter": "test-_/\\.%\\&\\#;1",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 11, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else if dateStart < 1448453100 {
			calc := (countermax - i + 1000) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

		} else if dateStart < 1448453160 {
			var calc float32
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			i = 1.0

		} else if dateStart < 1448453400 {
			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			calc := (countermax - i + 3000.0) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryFilterRateTrueNoPoints(t *testing.T) {

	payload := `{
		"start": 1348452800000,
		"end": 1348512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"rate": true,
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "test1",
				"groupBy": false
			}]
		}]
	}`

	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	assert.Equal(t, 200, code)
	assert.Equal(t, "[]", string(response))

}

// Tags

func TestTsdbQuery(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryAproxMediaExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "avg",
			"downsample": "3m-avg",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		media := float32((i + i + 1 + i + 2) / 3)
		assert.Exactly(t, media, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryAproxMin(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458500000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-min",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryAproxMinExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "min",
			"downsample": "3m-min",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryAproxMaxExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "max",
			"downsample": "3m-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 2.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryAproxSumExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "sum",
			"downsample": "3m-sum",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		sum := float32(i + i + 1 + i + 2)
		assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
		i++
	}
}

func TestTsdbQueryAproxCountExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458140000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts02tsdb",
			"aggregator": "sum",
			"downsample": "3m-count",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts02tsdb", 1, 30, 1, 0, 1, "ts2TsdbQuery3")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 3.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryAproxMaxHour(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1449316800000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts03tsdb",
			"aggregator": "max",
			"downsample": "2h-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts03tsdb", 1, 120, 1, 0, 1, "ts3TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 15.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 20.0

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 7200
	}
}

func TestTsdbQueryAproxMaxDay(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1449316800000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts03tsdb",
			"aggregator": "max",
			"downsample": "2d-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts03tsdb", 1, 6, 1, 0, 1, "ts3TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 355.0
	dateStart := 1448409600

	for _, key := range keys {

		date, _ := strconv.Atoi(key)

		if date < 1449100800 {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 480.0

		} else {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 120.0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 172800
	}
}

func TestTsdbQueryAproxMaxWeek(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1573646400000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "4w-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 52, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 1.5
	dateStart := time.Date(2015, 11, 23, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 2.0

		date := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(date), key)
		dateStart = dateStart.AddDate(0, 0, 28)
	}
}

func TestTsdbQueryAproxMaxMonth(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1566734400000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "3n-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 16, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 4.5
	dateStart := time.Date(2015, 11, 1, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		date, _ := strconv.Atoi(key)

		if date <= 1501545600 {

			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 6.5

		} else if date == 1509494400 {

			i = 57.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1517443200 {

			i = 63.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1525132800 {

			i += 6.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1533081600 {

			i += 7.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date <= 1541030400 {

			i += 6.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1548979200 {

			i += 6.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else if date == 1556668800 {

			i += 7.0
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			i += 1.5
			assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		}

		dateInt := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(dateInt), key)
		dateStart = dateStart.AddDate(0, 3, 0)
	}
}

func TestTsdbQueryAproxMaxYear(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452740000,
		"end": 1545825600000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts04tsdb",
			"aggregator": "max",
			"downsample": "1y-max",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts04tsdb", 1, 4, 1, 0, 1, "ts4TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32 = 2.5
	dateStart := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 26.0

		date := int(dateStart.Unix())

		assert.Exactly(t, strconv.Itoa(date), key)
		dateStart = dateStart.AddDate(1, 0, 0)
	}
}

func TestTsdbQueryMergeDateLimit(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458740000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_1tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_1tsdb", 1, 100, 0, 1, 2, "ts1_1TsdbQuery", "ts1_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, i+i, float32(payloadPoints[0].Dps[key].(float64)))
		i++

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeBiggerPointsSameDate(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts05tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts05tsdb", 1, 90, 0, 1, 3, "ts5TsdbQuery", "ts5TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts5TsdbQuery3"], "Tsuuid not found")

	var i float32
	dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, i*2, float32(payloadPoints[0].Dps[key].(float64)))
		i++

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeAvg(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"aggregator": "avg"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 90, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		avg := (i + j) / 2
		assert.Exactly(t, avg, float32(payloadPoints[0].Dps[key].(float64)))
		i++
		j += 5

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeMin(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"aggregator": "min"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 90, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryMergeMax(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"aggregator": "max"
		}]
	}`
	path := fmt.Sprintf("keyspaces/%s/api/query", ksMycenae)
	code, response, err := mycenaeTools.HTTP.POST(path, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 90, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].AggTags))
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids))
	assert.Equal(t, "ts07tsdb", payloadPoints[0].Metric)
	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts7TsdbQuery"], "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts7TsdbQuery2"], "Tsuuid not found")

	var i float32
	dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 5.0

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeSum(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 90, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		sum := i + j
		assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
		i++
		j += 5

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeCount(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"aggregator": "count"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 90, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	dateStart := 1448452800

	for _, key := range keys {

		assert.Exactly(t, float32(2.0), float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryMergeSumDownAvgMinute(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"downsample": "3m-avg",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 30, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		sum := (i+i+1+i+2)/3 + (j+j+5+j+10)/3
		assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3
		j += 15

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryMergeSumDownSumMinute(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"downsample": "3m-sum",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 30, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		sum := (i + i + 1 + i + 2) + (j + j + 5 + j + 10)
		assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3
		j += 15

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryMergeTimeDiff(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458770000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 200, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryMergeTimeDiffDownsampleExactBeginAndEnd(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458770000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"downsample": "3m-min",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 34, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i+i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryMergeTimeDiffDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800001,
		"end": 1448458740001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"downsample": "3m-min",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 34, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		if key == "1448452800" {
			assert.Exactly(t, float32(0.0), float32(payloadPoints[0].Dps[key].(float64)))
			i += 3

		} else if key == "1448458740" {
			assert.Exactly(t, float32(99.0), float32(payloadPoints[0].Dps[key].(float64)))
			i += 3

		} else {
			assert.Exactly(t, i+i, float32(payloadPoints[0].Dps[key].(float64)))
			i += 3
		}
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryNullValuesDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09tsdb",
			"downsample": "3m-sum",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09tsdb", 1, 10, 1, 0, 1)

	assert.Equal(t, hashMap["ts9TsdbQuery"], payloadPoints[0].Tsuuids[0])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			i = 75
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryNullValuesDownsampleNone(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09tsdb",
			"downsample": "3m-sum-none",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09tsdb", 1, 10, 1, 0, 1)

	assert.Equal(t, hashMap["ts9TsdbQuery"], payloadPoints[0].Tsuuids[0])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			i = 75
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryNullValuesDownsampleNull(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09tsdb",
			"downsample": "3m-sum-null",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09tsdb", 1, 30, 1, 0, 1)

	assert.Equal(t, hashMap["ts9TsdbQuery"], payloadPoints[0].Tsuuids[0])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			assert.Empty(t, payloadPoints[0].Dps[key], "they should be nil")
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryNullValuesDownsampleNan(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09tsdb",
			"downsample": "3m-sum-nan",
			"aggregator": "sum"
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []tools.ResponseQuery{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, "ts09tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 30, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 0, len(payloadPoints[0].AggTags))
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids))

	assert.Equal(t, hashMap["ts9TsdbQuery"], payloadPoints[0].Tsuuids[0])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	i := 0.0
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key].(float64))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key].(string), "they should be nan")
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryNullValuesDownsampleZero(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09tsdb",
			"downsample": "3m-sum-zero",
			"aggregator": "sum"
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []tools.ResponseQuery{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, "ts09tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 30, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 0, len(payloadPoints[0].AggTags))
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids))

	assert.Equal(t, hashMap["ts9TsdbQuery"], payloadPoints[0].Tsuuids[0])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	i := 0.0
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key].(float64))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			assert.Exactly(t, 0.0, payloadPoints[0].Dps[key], "they should be nil")
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryNullValueMerge(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 90, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + j
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Exactly(t, j, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i++
		j++

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
	}
}

func TestTsdbQueryBothValuesNullMerge(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 30, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + j
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 60

		} else {
			i = 75
			j = 75
			sum := i + j
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 60
		}

		i++
		j++
	}
}

func TestTsdbQueryBothValuesNullMergeAndDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"downsample": "3m-max",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 10, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			i = 75
			j = 75
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryBothValuesNullMergeAndDownsampleNone(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"downsample": "3m-max-none",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 10, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			i = 75
			j = 75
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryBothValuesNullMergeAndDownsampleNull(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"downsample": "3m-max-null",
			"aggregator": "sum"
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, "ts09_1tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 30, len(payloadPoints[0].Dps))
	assert.Equal(t, 0, len(payloadPoints[0].Tags))
	assert.Equal(t, 1, len(payloadPoints[0].AggTags))
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids))

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts9_1TsdbQuery2"], "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts9_1TsdbQuery4"], "Tsuuid not found")

	i, j := 0.0, 0.0
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key].(float64))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			assert.Exactly(t, nil, payloadPoints[0].Dps[key], "they should be nan")
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryBothValuesNullMergeAndDownsampleNan(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"downsample": "3m-max-nan",
			"aggregator": "sum"
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, "ts09_1tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 30, len(payloadPoints[0].Dps))
	assert.Equal(t, 0, len(payloadPoints[0].Tags))
	assert.Equal(t, 1, len(payloadPoints[0].AggTags))
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids))

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts9_1TsdbQuery2"], "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts9_1TsdbQuery4"], "Tsuuid not found")

	i, j := 0.0, 0.0
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key].(float64))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			//assert.Empty(t, payloadPoints[0].Dps[key], "they should be nil")
			//assert.Exactly(t, strconv.Itoa(dateStart), key)
			//dateStart += 180

			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key].(string), "they should be nan")
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryBothValuesNullMergeAndDownsampleZero(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"downsample": "3m-max-zero",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 30, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			assert.Exactly(t, 0.0, payloadPoints[0].Dps[key].(float64))
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryNullValueMergeAndDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryNullValueMergeAndDownsampleNone(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum-none",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryNullValueMergeAndDownsampleNull(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum-null",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryNullValueMergeAndDownsampleNan(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum-nan",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryNullValueMergeAndDownsampleZero(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum-zero",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 1, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryTwoTSMerge(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum",
			"aggregator": "sum"
		},{
			"metric": "ts07_1tsdb",
			"downsample": "3m-sum",
			"aggregator": "avg"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_1tsdb", 2, 30, 0, 1, 2, "ts7_1TsdbQuery", "ts7_1TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}

	assert.Equal(t, 0, len(payloadPoints[1].Tags))
	assert.Equal(t, 1, len(payloadPoints[1].AggTags))
	assert.Equal(t, 30, len(payloadPoints[1].Dps))
	assert.Equal(t, 2, len(payloadPoints[1].Tsuuids))
	assert.Equal(t, "ts07_1tsdb", payloadPoints[1].Metric)
	assert.Equal(t, "host", payloadPoints[1].AggTags[0])

	if hashMap["ts7_1TsdbQuery"] == payloadPoints[1].Tsuuids[0] {
		assert.Equal(t, hashMap["ts7_1TsdbQuery"], payloadPoints[1].Tsuuids[0])
		assert.Equal(t, hashMap["ts7_1TsdbQuery2"], payloadPoints[1].Tsuuids[1])
	} else {
		assert.Equal(t, hashMap["ts7_1TsdbQuery"], payloadPoints[1].Tsuuids[1])
		assert.Equal(t, hashMap["ts7_1TsdbQuery2"], payloadPoints[1].Tsuuids[0])
	}

	i = 0.0
	j = 0.0
	dateStart = 1448452800
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := ((i + i + 1 + i + 2) + (j + j + 1 + j + 2)) / 2
			assert.Exactly(t, sum, float32(payloadPoints[1].Dps[key].(float64)))

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, float32(payloadPoints[1].Dps[key].(float64)))

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryMoreThanOneTS(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		},{
			"metric": "ts10tsdb",
			"aggregator": "sum",
			"tags": {
				"host": "test2"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 2, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}

	keys = []string{}

	for key := range payloadPoints[1].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 25, len(payloadPoints[1].Dps))
	assert.Equal(t, 1, len(payloadPoints[1].Tags))
	assert.Equal(t, 0, len(payloadPoints[1].AggTags))
	assert.Equal(t, 1, len(payloadPoints[1].Tsuuids))
	assert.Equal(t, "ts10tsdb", payloadPoints[1].Metric)
	assert.Equal(t, "test2", payloadPoints[1].Tags["host"])
	assert.Equal(t, ts10IDTsdbQuery, payloadPoints[1].Tsuuids[0])

	i = 0.0
	dateStart = 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[1].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryTagsMoreThanOneTSOnlyOneExists(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		},{
			"metric": "nopoints",
			"aggregator": "sum",
			"tags": {
				"host": "test2"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryTagsMoreThanOneTSOnlyOneExists2(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "nopoints",
			"aggregator": "sum",
			"tags": {
				"host": "test2"
			}
		},{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterMoreThanOneTSOnlyOneExists(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		},{
			"metric": "nopoints",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test2",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterMoreThanOneTSOnlyOneExists2(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "nopoints",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		},{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterMoreThanOneTSOnlyOneHasPoints(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		},{
			"metric": "ts01_3tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test.2",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterMoreThanOneTSOnlyOneHasPoints2(t *testing.T) {
	t.Parallel()

	payload := `{
		"start": 1448452800000,
		"end": 1548452700000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_3tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test.2",
				"groupBy": false
			}]
		},{
			"metric": "ts01tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 100, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60
		i++
	}
}

func TestTsdbQueryFilterValuesGreaterThan(t *testing.T) {

	payload := `{
		"start": 1448452800001,
		"end": 1448458740001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"filterValue": "> 49",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 99, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32 = 50.0
	dateStart := 1448455800
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryFilterValuesGreaterThanEqualTo(t *testing.T) {

	payload := `{
		"start": 1448452800001,
		"end": 1448458740001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"filterValue": "> = 49",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 101, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32 = 49.0
	dateStart := 1448455740
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryFilterValuesLessThan(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458740000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"filterValue": "<50",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 100, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800
	count := 0
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		count++

		if count == 2 {
			i++
			count = 0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryFilterValuesLessThanEqualTo(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458740000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"filterValue": "<=50",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 102, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1448452800
	count := 0
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		count++

		if count == 2 {
			i++
			count = 0
		}

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryFilterValuesEqualTo(t *testing.T) {

	payload := `{
		"start": 1448452800001,
		"end": 1448458740001,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"filterValue": "==50",
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_2tsdb", 1, 2, 0, 1, 2, "ts1_2TsdbQuery", "ts1_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32 = 50.0
	dateStart := 1448455800
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 30
	}
}

func TestTsdbQueryRateTrueRateOptionsFalse(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"rate": true,
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 99, 1, 0, 1, "ts1TsdbQuery")

	assert.Equal(t, "test", payloadPoints[0].Tags["host"])

	var i float32
	dateStart := 1448452800
	for _, key := range keys {

		calc := ((i + 1.0) - i) / float32((dateStart+60)-dateStart)
		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		dateStart += 60
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i++
	}
}

func TestTsdbQueryRateTrueAndMergeRateOptionsFalse(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"rate": true,
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 89, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// Rate: (v2 - v1) / (t2 - t1)
		calc := (((i + 1) + (j + 5)) - (i + j)) / float32((dateStart+60)-dateStart)

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i++
		j += 5

		dateStart += 60
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryRateTrueDownsampleAndMergeRateOptionsFalse(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07tsdb", 1, 29, 0, 1, 2, "ts7TsdbQuery", "ts7TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// Rate: (v2 - v1) / (t2 - t1)
		calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)) / (float32((dateStart + 180) - dateStart))

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryRateFillNone(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"rate": true,
			"downsample": "3m-avg-none",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 9, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452980
	for _, key := range keys {

		if i < 12 || i > 15 {
			//sum := i + 2 + j + 2
			//assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180

		} else {
			i = 75
			j = 75
			//sum := i + 2 + j + 2
			//assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart = 1448457300
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}

		i += 3
		j += 3
	}
}

func TestTsdbQueryRateFillZero(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"rate": true,
			"downsample": "3m-avg-zero",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:	Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else if i == 12 {
			calc := (0 - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else if i == 72 {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - 0) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Exactly(t, float32(0.0), float32(payloadPoints[0].Dps[key].(float64)))
		}
		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryRateFillNull(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"rate": true,
			"downsample": "3m-avg-null",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:	Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Nil(t, payloadPoints[0].Dps[key])
		}
		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryRateFillNan(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts09_1tsdb",
			"rate": true,
			"downsample": "3m-avg-nan",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:	Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key].(string))
		}
		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryOrderDownsampleMergeRateAndFilterValues(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"filterValue": ">5",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum",
			"order":["filterValue","downsample","aggregation","rate"]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_2tsdb", 1, 29, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:	Merge - Downsample - Rate

		//first point
		if key == "1448452980" {
			calc := 10 / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
			//second point
		} else if key == "1448453160" {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((j + j + 5 + j + 10) / 3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)) / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryOrderDownsampleRateAndMerge(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum",
			"order":["downsample","rate","aggregation"]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_2tsdb", 1, 29, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:	Downsample - Rate - Merge
		calc := (((((i + 3) + (i + 3) + 1 + (i + 3) + 2) / 3) - (i+i+1+i+2)/3) / float32((dateStart+180)-dateStart)) +
			(((((j + 15) + (j + 15) + 5 + (j + 15) + 10) / 3) - (j+j+5+j+10)/3) / float32((dateStart+180)-dateStart))

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryOrderMergeDownsampleAndRate(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum",
			"order":["aggregation","downsample","rate"]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_2tsdb", 1, 29, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Downsample - Rate
		calc := (((i+3+j+15)+(i+3+1+j+15+5)+(i+3+2+j+15+10))/6 - ((i+j)+(i+1+j+5)+(i+2+j+10))/6) / float32((dateStart+180)-dateStart)

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryOrderMergeRateAndDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum",
			"order":["aggregation","rate","downsample"]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_2tsdb", 1, 30, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	dateStart := 1448452800
	count := 0
	var calc, i, j float64

	for _, key := range keys {
		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Rate - Downsample

		if count == 0 {
			calc = (((j - i) / 1) +
				(((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1)) / 5

			assert.Exactly(t, roundValue(calc), roundValue(payloadPoints[0].Dps[key].(float64)))
			i += 2
			j += 10

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
			count = 1

		} else {
			calc = ((((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1) +
				(((i + 3) - (j + 10)) / 59) +
				((j + 15) - (i+3)/1)) / 6

			assert.Exactly(t, roundValue(calc), roundValue(payloadPoints[0].Dps[key].(float64)))
			i += 3
			j += 15

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
	}
}

/*
Falling Test
func TestTsdbQueryOrderRateMergeAndDownsample(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"aggregator": "sum",
			"order":["rate","aggregation","downsample"]
		}]
	}`

	keys, payloadPoints := postApiQueryAndCheck(t, payload, "ts07_2tsdb", 1, 30, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {
		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Rate - Merge - Downsample

		calc := ((((i + 1) - (i)) / float32(dateStart+60-dateStart)) +
			(((j + 5) - (j)) / float32(dateStart+60-dateStart)) +
			(((i + 2) - (i + 1)) / float32(dateStart+120-dateStart+60)) +
			(((j + 10) - (j + 5)) / float32(dateStart+120-dateStart+60)) +
			(((i + 3) - (i + 2)) / float32(dateStart+180-dateStart+120)) +
			(((j + 15) - (j + 10)) / float32(dateStart+180-dateStart+120))) / 6

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i += 1
		j += 15

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryOrderRateDownsampleAndMerge(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"aggregator": "sum",
			"order":["rate","downsample","aggregation"]
		}]
	}`

	keys, payloadPoints := postApiQueryAndCheck(t, payload, "ts07_2tsdb", 1, 30, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Rate - Downsample - Merge
		calc := ((((i+1)-(i))/float32(dateStart+60-dateStart))+
			(((i+2)-(i+1))/float32(dateStart+120-dateStart+60))+
			(((i+3)-(i+2))/float32(dateStart+180-dateStart+120)))/3 +
			((((j+5)-(j))/float32(dateStart+60-dateStart))+
				(((j+10)-(j+5))/float32(dateStart+120-dateStart+60))+
				(((j+15)-(j+10))/float32(dateStart+180-dateStart+120)))/3

		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
		i += 1
		j += 15

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}
*/

func TestTsdbQueryDefaultOrder(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"filterValue": ">5",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum"
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	assert.Equal(t, "ts07_2tsdb", payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))
	assert.Equal(t, 29, len(payloadPoints[0].Dps))
	assert.Equal(t, 0, len(payloadPoints[0].Tags))
	assert.Equal(t, 1, len(payloadPoints[0].AggTags))
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids))

	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts7_2TsdbQuery"], "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, hashMap["ts7_2TsdbQuery2"], "Tsuuid not found")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float32
	dateStart := 1448452800

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:	Merge - Downsample - Rate

		//first point
		if key == "1448452980" {
			calc := 10 / float32((dateStart+180)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
			//second point
		} else if key == "1448453160" {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((j + j + 5 + j + 10) / 3)) / (float32((dateStart + 180) - dateStart))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)) / (float32((dateStart + 180) - dateStart))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		}
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.Itoa(dateStart), key)

	}
}

func TestTsdbQueryOrderFunctionNotUsed(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts07_2tsdb",
			"rate": true,
			"downsample": "3m-avg",
			"rateOptions": {
				"counter": false
			},
			"aggregator": "sum",
			"order":["aggregation","rate","filterValue","downsample"]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts07_2tsdb", 1, 30, 0, 1, 2, "ts7_2TsdbQuery", "ts7_2TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i, j float64
	dateStart := 1448452800
	count := 0

	for _, key := range keys {
		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Rate - Downsample

		if count == 0 {
			calc := (((j - i) / 1) +
				(((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1)) / 5

			assert.Exactly(t, roundValue(calc), roundValue(payloadPoints[0].Dps[key].(float64)))
			i += 2
			j += 10

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
			count = 1

		} else {
			calc := ((((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1) +
				(((i + 3) - (j + 10)) / 59) +
				((j + 15) - (i+3)/1)) / 6

			assert.Exactly(t, roundValue(calc), roundValue(payloadPoints[0].Dps[key].(float64)))
			i += 3
			j += 15

			assert.Exactly(t, strconv.Itoa(dateStart), key)
			dateStart += 180
		}
	}
}

func TestTsdbQueryRateTrueRateOptionsTrueCounter(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453040000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true
			},
			"aggregator": "sum",
			"tags": {
				"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 4, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		dateStart += 60
		assert.Exactly(t, strconv.Itoa(dateStart), key)

		i = i * 10.0
	}
}

func TestTsdbQueryRateTrueRateOptionsTrueCounterMax(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453100000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"counterMax": 15000
			},
			"aggregator": "sum",
			"tags": {
				"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			calc := (countermax - i + 1000) / (float32(dateStart + 60 - dateStart))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryRateTrueRateOptionsTrueResetValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453100000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"resetValue": 10
			},
			"aggregator": "sum",
			"tags": {
				"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			var calc float32
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryRateTrueRateOptionsTrueCounterMaxAndResetValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453460000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts12-_/.%&#;tsdb",
			"rate": true,
			"rateOptions": {
				"counter": true,
				"counterMax": 15000,
				"resetValue": 233
			},
			"aggregator": "sum",
			"tags": {
				"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 11, 1, 0, 1)

	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])

	var i float32 = 1.0
	dateStart := 1448452800
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < 1448453040 {

			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else if dateStart < 1448453100 {
			calc := (countermax - i + 1000) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

		} else if dateStart < 1448453160 {
			var calc float32
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
			i = 1.0

		} else if dateStart < 1448453400 {
			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)

			i = i * 10.0
		} else {
			calc := (countermax - i + 3000.0) / float32((dateStart+60)-dateStart)
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

			dateStart += 60
			assert.Exactly(t, strconv.Itoa(dateStart), key)
		}
	}
}

func TestTsdbQueryRateTrueNoPoints(t *testing.T) {

	payload := `{
		"start": 1348452800000,
		"end": 1348512200000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01tsdb",
			"rate": true,
			"aggregator": "sum",
			"tags": {
				"host": "test"
			}
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Exactly(t, "[]", string(response))

}

func TestTsdbQueryFilterGroupByWildcard(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByWildcardPartialName(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "hos*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByWildcardTagWithDot(t *testing.T) {

	payload := `{
		"start": 1348452800000,
		"end": 1348458770000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_3tsdb",
			"aggregator": "sum",
			"downsample": "3m-min",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test.*",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_3tsdb", 1, 34, 0, 1, 2, "ts1_3TsdbQuery", "ts1_3TsdbQuery2")

	assert.Equal(t, "host", payloadPoints[0].AggTags[0])

	var i float32
	dateStart := 1348452780
	for _, key := range keys {

		assert.Exactly(t, i+i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryFilterGroupByWildcardTagWithDotPartialName(t *testing.T) {

	payload := `{
		"start": 1348452800000,
		"end": 1348458770000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_3tsdb",
			"aggregator": "sum",
			"downsample": "3m-min",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test.2*",
				"groupBy": false
			}]
		}]
	}`

	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01_3tsdb", 1, 34, 1, 0, 1)

	assert.Equal(t, "test.2", payloadPoints[0].Tags["host"])
	assert.Equal(t, hashMap["ts1_3TsdbQuery2"], payloadPoints[0].Tsuuids[0])

	var i float32
	dateStart := 1348452780
	for _, key := range keys {

		assert.Exactly(t, i, float32(payloadPoints[0].Dps[key].(float64)))
		i += 3

		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 180
	}
}

func TestTsdbQueryFilterGroupByWildcardTagWithoutDot(t *testing.T) {

	payload := `{
		"start": 1448452800000,
		"end": 1448458770000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts01_2tsdb",
			"aggregator": "sum",
			"downsample": "3m-min",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "test.*",
				"groupBy": false
			}]
		}]
	}`

	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	assert.Equal(t, 200, code)
	assert.Equal(t, "[]", string(response))

}

func TestTsdbQueryFilterGroupByLiteralOr(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "literal_or",
				"tagk": "host",
				"filter": "host1|host2",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 2, len(payloadPoints))

	for i := 0; i < 2; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByNotLiteralOr(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "not_literal_or",
				"tagk": "host",
				"filter": "host1|host2",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 20, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 2, len(payloadPoints[0].AggTags))
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids))
	assert.Equal(t, "ts13tsdb", payloadPoints[0].Metric)
	assert.Equal(t, "app", payloadPoints[0].AggTags[0])
	assert.Equal(t, "type", payloadPoints[0].AggTags[1])
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"])

	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

	var value float32 = 26.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, value, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		value += 26.0
	}
}

func TestTsdbQueryFilterGroupByRegexp(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": ".*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByRegexpNumbers(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "regexp",
				"tagk": "host",
				"filter": "host[0-9]{1}",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByILiteralOr(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "iliteral_or",
				"tagk": "host",
				"filter": "host1|host2|host3",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQuery3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQuery4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQuery8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByAllEspecificTag(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}
}

func TestTsdbQueryFilterGroupByIsntTheFirstFilter(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			},{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5, ts13IDTsdbQuery6:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}
}

func TestTsdbQueryFilterGroupByLiteralOrEspecificTag(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "literal_or",
				"tagk": "host",
				"filter": "host1|host2",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 2, len(payloadPoints))

	for i := 0; i < 2; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery, ts13IDTsdbQuery2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQuery3, ts13IDTsdbQuery4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}
}

func TestTsdbQueryFilterGroupByWildcardTwoTags(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "type",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 5, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQuery3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		case ts13IDTsdbQuery7:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type4", payloadPoints[i].Tags["type"])

			assert.Equal(t, ts13IDTsdbQuery7, payloadPoints[i].Tsuuids[0])

			var value float32 = 7.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQuery8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type5", payloadPoints[i].Tags["type"])

			assert.Equal(t, ts13IDTsdbQuery8, payloadPoints[i].Tsuuids[0])

			var value float32 = 8.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 8.0

			}

		default:
			t.Error("not found")
		}

	}
}

func TestTsdbQueryFilterGroupByWildcardTwoTagsFiltersOutOfOrder1(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "type",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQuery3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryFilterGroupByWildcardTwoTagsFiltersOutOfOrder2(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			},{
				"type": "wildcard",
				"tagk": "type",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQuery3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryFilterGroupByWildcardTwoTagsFiltersOutOfOrder3(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "app",
				"filter": "app1",
				"groupBy": false
			},{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "type",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, len(payloadPoints))

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQuery:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQuery3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQuery5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdb", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := 1448452800
			for _, key := range keys {

				assert.Exactly(t, value, float32(payloadPoints[i].Dps[key].(float64)))

				assert.Exactly(t, strconv.Itoa(dateStart), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryFilterGroupBySameTagk(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "host",
				"filter": "host3",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	sort.Strings(payloadPoints[0].AggTags)

	assert.Equal(t, 20, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 2, len(payloadPoints[0].AggTags))
	assert.Equal(t, "app", payloadPoints[0].AggTags[0])
	assert.Equal(t, "type", payloadPoints[0].AggTags[1])
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids))
	assert.Equal(t, "ts13tsdb", payloadPoints[0].Metric)
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"])

	tsuuidExpected := []string{ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8}
	tsuuidActual := []string{payloadPoints[0].Tsuuids[0], payloadPoints[0].Tsuuids[1],
		payloadPoints[0].Tsuuids[2], payloadPoints[0].Tsuuids[3]}

	sort.Strings(tsuuidExpected)
	sort.Strings(tsuuidActual)

	assert.Equal(t, tsuuidExpected, tsuuidActual)

	var value float32 = 26.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, value, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		value += 26.0

	}
}

func TestTsdbQueryFilterSameTagkOnGroupByAndTags(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			},{
				"type": "wildcard",
				"tagk": "host",
				"filter": "host3",
				"groupBy": false
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, len(payloadPoints))

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	sort.Strings(payloadPoints[0].AggTags)

	assert.Equal(t, 20, len(payloadPoints[0].Dps))
	assert.Equal(t, 1, len(payloadPoints[0].Tags))
	assert.Equal(t, 2, len(payloadPoints[0].AggTags))
	assert.Equal(t, "app", payloadPoints[0].AggTags[0])
	assert.Equal(t, "type", payloadPoints[0].AggTags[1])
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids))
	assert.Equal(t, "ts13tsdb", payloadPoints[0].Metric)
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"])

	tsuuidExpected := []string{ts13IDTsdbQuery5, ts13IDTsdbQuery6, ts13IDTsdbQuery7, ts13IDTsdbQuery8}
	tsuuidActual := []string{payloadPoints[0].Tsuuids[0], payloadPoints[0].Tsuuids[1],
		payloadPoints[0].Tsuuids[2], payloadPoints[0].Tsuuids[3]}

	sort.Strings(tsuuidExpected)
	sort.Strings(tsuuidActual)

	assert.Equal(t, tsuuidExpected, tsuuidActual)

	var value float32 = 26.0
	dateStart := 1448452800
	for _, key := range keys {

		assert.Exactly(t, value, float32(payloadPoints[0].Dps[key].(float64)))
		assert.Exactly(t, strconv.Itoa(dateStart), key)
		dateStart += 60

		value += 26.0

	}
}

func TestTsdbQueryFilterGroupByNoPoints(t *testing.T) {

	payload := `{
		"start": 1348452740000,
		"end": 1348453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "host",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Exactly(t, "[]", string(response))
}

func TestTsdbQueryFilterGroupByTagNotFound(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
			"metric": "ts13tsdb",
			"aggregator": "sum",
			"filters": [{
				"type": "wildcard",
				"tagk": "x",
				"filter": "*",
				"groupBy": true
			}]
		}]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := []tools.ResponseQuery{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, "[]", string(response))

}

//func TestTsdbQueryShowQuery(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448453940000,
//		"queries": [{
//			"metric": "ts17tsdb",
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts17tsdb", 1, 5, 0, 1, 0)
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	i := 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts17tsdb",
//		Filters:    []structs.TSDBfilter{},
//		Tags:       map[string]string{},
//		Index:      &i,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	dateStart := 1448452800
//	for i, key := range keys {
//
//		assert.Exactly(t, float64(i+i), payloadPoints[0].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//	}
//
//}
//
//func TestTsdbQueryShowQueryNoPoints(t *testing.T) {
//
//	payload := `{
//		"start": 1448453100000,
//		"end": 1448453340000,
//		"queries": [{
//			"metric": "ts17tsdb",
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	assert.Equal(t, 200, code)
//	assert.Equal(t, "[]", string(response))
//
//}
//
//func TestTsdbQueryShowQueryWithDownsample(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448453940000,
//		"queries": [{
//			"metric": "ts17tsdb",
//			"aggregator": "sum",
//			"downsample": "5m-sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts17tsdb", 1, 1, 0, 1, 0)
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	d, i := "5m-sum", 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts17tsdb",
//		Downsample: &d,
//		Filters:    []structs.TSDBfilter{},
//		Tags:       map[string]string{},
//		Index:      &i,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	assert.Exactly(t, float64(20), payloadPoints[0].Dps[keys[0]])
//	assert.Exactly(t, strconv.Itoa(1448452800), keys[0])
//
//}
//
//func TestTsdbQueryShowQueryRateFillNone(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts09_1tsdb",
//			"rate": true,
//			"downsample": "3m-avg-none",
//			"rateOptions": {
//				"counter": false
//			},
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 9, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	d, index := "3m-avg-none", 0
//	expected := structs.TSDBquery{
//		Aggregator:  "sum",
//		Metric:      "ts09_1tsdb",
//		Downsample:  &d,
//		Filters:     []structs.TSDBfilter{},
//		Tags:        map[string]string{},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i, j float32
//	dateStart := 1448452980
//	for _, key := range keys {
//
//		if i < 12 || i > 15 {
//			//sum := i + 2 + j + 2
//			//assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//			dateStart += 180
//
//		} else {
//			i = 75
//			j = 75
//			//sum := i + 2 + j + 2
//			//assert.Exactly(t, sum, float32(payloadPoints[0].Dps[key].(float64)))
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart = 1448457300
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//			dateStart += 180
//		}
//
//		i += 3
//		j += 3
//	}
//}
//
//func TestTsdbQueryShowQueryRateFillZero(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts09_1tsdb",
//			"rate": true,
//			"downsample": "3m-avg-zero",
//			"rateOptions": {
//				"counter": false
//			},
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	d, index := "3m-avg-zero", 0
//	expected := structs.TSDBquery{
//		Aggregator:  "sum",
//		Metric:      "ts09_1tsdb",
//		Downsample:  &d,
//		Filters:     []structs.TSDBfilter{},
//		Tags:        map[string]string{},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i, j float32
//	dateStart := 1448452800
//
//	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
//	// Rate: (v2 - v1) / (t2 - t1)
//	// Downsample: 3min
//	// DefaultOrder:	Merge - Downsample - Rate
//	for _, key := range keys {
//
//		if i < 12 || i >= 75 {
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		} else if i == 12 {
//			calc := (0 - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		} else if i == 72 {
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - 0) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		} else {
//			assert.Exactly(t, float32(0.0), float32(payloadPoints[0].Dps[key].(float64)))
//		}
//		dateStart += 180
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		i += 3
//		j += 3
//	}
//}
//
//func TestTsdbQueryShowQueryRateFillNull(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts09_1tsdb",
//			"rate": true,
//			"downsample": "3m-avg-null",
//			"rateOptions": {
//				"counter": false
//			},
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	d, index := "3m-avg-null", 0
//	expected := structs.TSDBquery{
//		Aggregator:  "sum",
//		Metric:      "ts09_1tsdb",
//		Downsample:  &d,
//		Filters:     []structs.TSDBfilter{},
//		Tags:        map[string]string{},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i, j float32
//	dateStart := 1448452800
//
//	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
//	// Rate: (v2 - v1) / (t2 - t1)
//	// Downsample: 3min
//	// DefaultOrder:	Merge - Downsample - Rate
//	for _, key := range keys {
//
//		if i < 12 || i >= 75 {
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		} else {
//			assert.Nil(t, payloadPoints[0].Dps[key])
//		}
//		dateStart += 180
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		i += 3
//		j += 3
//	}
//}
//
//func TestTsdbQueryShowQueryRateFillNan(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts09_1tsdb",
//			"rate": true,
//			"downsample": "3m-avg-nan",
//			"rateOptions": {
//				"counter": false
//			},
//			"aggregator": "sum"
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts09_1tsdb", 1, 29, 0, 1, 2, "ts9_1TsdbQuery2", "ts9_1TsdbQuery4")
//
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//
//	d, index := "3m-avg-nan", 0
//	expected := structs.TSDBquery{
//		Aggregator:  "sum",
//		Metric:      "ts09_1tsdb",
//		Downsample:  &d,
//		Filters:     []structs.TSDBfilter{},
//		Tags:        map[string]string{},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i, j float32
//	dateStart := 1448452800
//
//	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
//	// Rate: (v2 - v1) / (t2 - t1)
//	// Downsample: 3min
//	// DefaultOrder:	Merge - Downsample - Rate
//	for _, key := range keys {
//
//		if i < 12 || i >= 75 {
//			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / float32((dateStart+180)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		} else {
//			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key].(string))
//		}
//		dateStart += 180
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		i += 3
//		j += 3
//	}
//}
//
//func TestTsdbQueryShowQueryWithFilter(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448453940000,
//		"queries": [{
//			"metric": "ts17tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			}]
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts17tsdb", 2, 5, 1, 0, 0)
//
//	for i := 0; i < 2; i++ {
//
//		index := 0
//		expected := structs.TSDBquery{
//			Aggregator: "avg",
//			Metric:     "ts17tsdb",
//			Filters: []structs.TSDBfilter{{
//				Tagk:        "host",
//				Filter:      "*",
//				GroupByResp: true,
//				Ftype:       "wildcard",
//			}},
//			Tags:  map[string]string{},
//			Index: &index,
//		}
//		testSubQuery(t, expected, *payloadPoints[i].Query)
//
//		dateStart := 1448452800
//		for j, key := range keys {
//
//			assert.Exactly(t, float64(j), payloadPoints[i].Dps[key])
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//			dateStart += 60
//		}
//	}
//
//}
//
//func TestTsdbQueryShowQueryFilterGroupByTagNotFound(t *testing.T) {
//
//	payload := `{
//			"start": 1448452800000,
//			"end": 1448453940000,
//			"queries": [{
//				"metric": "ts17tsdb",
//				"aggregator": "avg",
//				"filters": [{
//					"type": "wildcard",
//					"tagk": "x",
//					"filter": "*",
//					"groupBy": true
//				}]
//			}],
//			"showQuery": true
//		}`
//
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//	payloadPoints := []tools.ResponseQuery{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	assert.Equal(t, 200, code)
//	assert.Equal(t, "[]", string(response))
//
//}
//
//func TestTsdbQueryShowQueryWithTwoFilters(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			},{
//				"type": "wildcard",
//				"tagk": "app",
//				"filter": "*",
//				"groupBy": true
//			}]
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts10tsdb", 1, 25, 2, 0, 0)
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "avg",
//		Metric:     "ts10tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "host",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}, {
//			Tagk:        "app",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}},
//		Tags:  map[string]string{},
//		Index: &index,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	dateStart := 1448452800
//	var i float64 = 1
//	for _, key := range keys {
//
//		assert.Exactly(t, i, payloadPoints[0].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//		i += 2
//	}
//}
//
//func TestTsdbQueryShowQueryWithFilterAndTwoQueries(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448453940000,
//		"queries": [{
//			"metric": "ts17tsdb",
//			"aggregator": "sum"
//		},{
//			"metric": "ts17tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			}]
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts17tsdb", 3, 5, 0, 1, 0)
//
//	for i := 0; i <= 2; i++ {
//
//		if i == 0 {
//
//			assert.Equal(t, "host", payloadPoints[i].AggTags[0])
//
//			expected := structs.TSDBquery{
//				Aggregator: "sum",
//				Metric:     "ts17tsdb",
//				Filters:    []structs.TSDBfilter{},
//				Tags:       map[string]string{},
//				Index:      &i,
//			}
//			testSubQuery(t, expected, *payloadPoints[i].Query)
//
//			dateStart := 1448452800
//			for j, key := range keys {
//
//				assert.Exactly(t, float64(j+j), payloadPoints[i].Dps[key])
//				assert.Exactly(t, strconv.Itoa(dateStart), key)
//				dateStart += 60
//			}
//
//		} else {
//
//			assert.Equal(t, 5, len(payloadPoints[i].Dps))
//			assert.Equal(t, 1, len(payloadPoints[i].Tags))
//			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
//			assert.Equal(t, 0, len(payloadPoints[i].Tsuuids))
//
//			index := 1
//			expected := structs.TSDBquery{
//				Aggregator: "avg",
//				Metric:     "ts17tsdb",
//				Filters: []structs.TSDBfilter{{
//					Tagk:        "host",
//					Filter:      "*",
//					GroupByResp: true,
//					Ftype:       "wildcard",
//				}},
//				Tags:  map[string]string{},
//				Index: &index,
//			}
//			testSubQuery(t, expected, *payloadPoints[i].Query)
//
//			dateStart := 1448452800
//			for j, key := range keys {
//
//				assert.Exactly(t, float64(j), payloadPoints[i].Dps[key])
//				assert.Exactly(t, strconv.Itoa(dateStart), key)
//				dateStart += 60
//			}
//		}
//	}
//}
//
//func TestTsdbQueryShowQueryWithExplicitTags(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			}],
//			"explicitTags": true
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts10tsdb", 1, 25, 1, 0, 0)
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "avg",
//		Metric:     "ts10tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "host",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}},
//		Tags:         map[string]string{},
//		Index:        &index,
//		ExplicitTags: true,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	dateStart := 1448452800
//	for i, key := range keys {
//
//		assert.Exactly(t, float64(i), payloadPoints[0].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//	}
//
//}
//
//func TestTsdbQueryShowQueryWithExplicitTagsNoMatch(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "app",
//				"filter": "*",
//				"groupBy": true
//			}],
//			"explicitTags": true
//		}],
//		"showQuery": true
//	}`
//
//	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	assert.Equal(t, 200, code)
//	assert.Equal(t, "[]", string(response))
//
//}
//
//func TestTsdbQueryShowQueryWithExplicitTagsNoPoints(t *testing.T) {
//
//	payload := `{
//		"start": 1448454300000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			}],
//			"explicitTags": true
//		}],
//		"showQuery": true
//	}`
//
//	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	assert.Equal(t, 200, code)
//	assert.Equal(t, "[]", string(response))
//
//}
//
//func TestTsdbQueryShowQueryWithTwoFiltersAndExplicitTags(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			},{
//				"type": "wildcard",
//				"tagk": "app",
//				"filter": "*",
//				"groupBy": true
//			}],
//			"explicitTags": true
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts10tsdb", 1, 25, 2, 0, 0)
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "avg",
//		Metric:     "ts10tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "host",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}, {
//			Tagk:        "app",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}},
//		Tags:         map[string]string{},
//		Index:        &index,
//		ExplicitTags: true,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	dateStart := 1448452800
//	var i float64 = 1
//	for _, key := range keys {
//
//		assert.Exactly(t, i, payloadPoints[0].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//		i += 2
//	}
//}
//
//func TestTsdbQueryShowQueryOneExplicitTagAndTwoQueries(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1548452700000,
//		"queries": [{
//			"metric": "ts10tsdb",
//			"aggregator": "sum"
//		},{
//			"metric": "ts10tsdb",
//			"aggregator": "avg",
//			"filters": [{
//				"type": "wildcard",
//				"tagk": "host",
//				"filter": "*",
//				"groupBy": true
//			}],
//			"explicitTags": true
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts10tsdb", 2, 25, 1, 1, 0)
//
//	//First query
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0])
//	index0 := 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts10tsdb",
//		Filters:    []structs.TSDBfilter{},
//		Tags:       map[string]string{},
//		Index:      &index0,
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	dateStart := 1448452800
//	for j, key := range keys {
//
//		assert.Exactly(t, float64(3*j+1), payloadPoints[0].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//	}
//
//	//Second query
//	assert.Equal(t, 25, len(payloadPoints[1].Dps))
//	assert.Equal(t, 1, len(payloadPoints[1].Tags))
//	assert.Equal(t, 0, len(payloadPoints[1].AggTags))
//	assert.Equal(t, 0, len(payloadPoints[1].Tsuuids))
//
//	index1 := 1
//	expected = structs.TSDBquery{
//		Aggregator: "avg",
//		Metric:     "ts10tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "host",
//			Filter:      "*",
//			GroupByResp: true,
//			Ftype:       "wildcard",
//		}},
//		Tags:         map[string]string{},
//		Index:        &index1,
//		ExplicitTags: true,
//	}
//	testSubQuery(t, expected, *payloadPoints[1].Query)
//
//	dateStart = 1448452800
//	for j, key := range keys {
//
//		assert.Exactly(t, float64(j), payloadPoints[1].Dps[key])
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//		dateStart += 60
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueRateOptionsFalse(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448512200000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts01tsdb",
//			"rate": true,
//			"aggregator": "sum",
//			"tags": {
//				"host": "test"
//			}
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts01tsdb", 1, 99, 1, 0, 1, "ts1TsdbQuery")
//
//	assert.Equal(t, "test", payloadPoints[0].Tags["host"])
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts01tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "host",
//			Filter:      "test",
//			GroupByResp: false,
//			Ftype:       "wildcard",
//		}},
//		Tags:        map[string]string{"host": "test"},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i float32
//	dateStart := 1448452800
//	for _, key := range keys {
//
//		calc := ((i + 1.0) - i) / float32((dateStart+60)-dateStart)
//		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		dateStart += 60
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		i++
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueRateOptionsTrueCounter(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1448453040000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts12-_/.%&#;tsdb",
//			"rate": true,
//			"rateOptions": {
//				"counter": true
//			},
//			"aggregator": "sum",
//			"tags": {
//				"hos-_/.%&#;t": "test-_/.%&#;1"
//			}
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 4, 1, 0, 1)
//
//	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
//	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts12-_/.%&#;tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "hos-_/.%&#;t",
//			Filter:      "test-_/.%&#;1",
//			GroupByResp: false,
//			Ftype:       "wildcard",
//		}},
//		Tags:        map[string]string{"hos-_/.%&#;t": "test-_/.%&#;1"},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{Counter: true},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i float32 = 1.0
//	dateStart := 1448452800
//	for _, key := range keys {
//
//		calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
//		assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//		dateStart += 60
//		assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		i = i * 10.0
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueRateOptionsTrueCounterMax(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1448453100000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts12-_/.%&#;tsdb",
//			"rate": true,
//			"rateOptions": {
//				"counter": true,
//				"counterMax": 15000
//			},
//			"aggregator": "sum",
//			"tags": {
//				"hos-_/.%&#;t": "test-_/.%&#;1"
//			}
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)
//
//	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
//	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])
//
//	index, cM := 0, int64(15000)
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts12-_/.%&#;tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "hos-_/.%&#;t",
//			Filter:      "test-_/.%&#;1",
//			GroupByResp: false,
//			Ftype:       "wildcard",
//		}},
//		Tags:        map[string]string{"hos-_/.%&#;t": "test-_/.%&#;1"},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{Counter: true, CounterMax: &cM},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i float32 = 1.0
//	dateStart := 1448452800
//	var countermax float32 = 15000.0
//	for _, key := range keys {
//
//		if dateStart < 1448453040 {
//
//			calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//			i = i * 10.0
//		} else {
//			calc := (countermax - i + 1000) / (float32(dateStart + 60 - dateStart))
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//		}
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueRateOptionsTrueResetValue(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1448453100000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts12-_/.%&#;tsdb",
//			"rate": true,
//			"rateOptions": {
//				"counter": true,
//				"resetValue": 10
//			},
//			"aggregator": "sum",
//			"tags": {
//				"hos-_/.%&#;t": "test-_/.%&#;1"
//			}
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1)
//
//	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
//	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])
//
//	index := 0
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts12-_/.%&#;tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "hos-_/.%&#;t",
//			Filter:      "test-_/.%&#;1",
//			GroupByResp: false,
//			Ftype:       "wildcard",
//		}},
//		Tags:        map[string]string{"hos-_/.%&#;t": "test-_/.%&#;1"},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{Counter: true, ResetValue: 10},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i float32 = 1.0
//	dateStart := 1448452800
//	for _, key := range keys {
//
//		if dateStart < 1448453040 {
//
//			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//			i = i * 10.0
//		} else {
//			var calc float32
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//		}
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueRateOptionsTrueCounterMaxAndResetValue(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1448453460000,
//		"showTSUIDs": true,
//		"queries": [{
//			"metric": "ts12-_/.%&#;tsdb",
//			"rate": true,
//			"rateOptions": {
//				"counter": true,
//				"counterMax": 15000,
//				"resetValue": 233
//			},
//			"aggregator": "sum",
//			"tags": {
//				"hos-_/.%&#;t": "test-_/.%&#;1"
//			}
//		}],
//		"showQuery": true
//	}`
//
//	keys, payloadPoints := postAPIQueryAndCheck(t, payload, "ts12-_/.%&#;tsdb", 1, 11, 1, 0, 1)
//
//	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"])
//	assert.Equal(t, ts12IDTsdbQuery, payloadPoints[0].Tsuuids[0])
//
//	index, cM := 0, int64(15000)
//	expected := structs.TSDBquery{
//		Aggregator: "sum",
//		Metric:     "ts12-_/.%&#;tsdb",
//		Filters: []structs.TSDBfilter{{
//			Tagk:        "hos-_/.%&#;t",
//			Filter:      "test-_/.%&#;1",
//			GroupByResp: false,
//			Ftype:       "wildcard",
//		}},
//		Tags:        map[string]string{"hos-_/.%&#;t": "test-_/.%&#;1"},
//		Index:       &index,
//		Rate:        true,
//		RateOptions: &structs.TSDBrateOptions{true, &cM, 233},
//	}
//	testSubQuery(t, expected, *payloadPoints[0].Query)
//
//	var i float32 = 1.0
//	dateStart := 1448452800
//	var countermax float32 = 15000.0
//	for _, key := range keys {
//
//		if dateStart < 1448453040 {
//
//			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//			i = i * 10.0
//		} else if dateStart < 1448453100 {
//			calc := (countermax - i + 1000) / float32((dateStart+60)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//		} else if dateStart < 1448453160 {
//			var calc float32
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//			i = 1.0
//
//		} else if dateStart < 1448453400 {
//			calc := ((i * 10.0) - i) / float32((dateStart+60)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//
//			i = i * 10.0
//		} else {
//			calc := (countermax - i + 3000.0) / float32((dateStart+60)-dateStart)
//			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))
//
//			dateStart += 60
//			assert.Exactly(t, strconv.Itoa(dateStart), key)
//		}
//	}
//}
//
//func TestTsdbQueryShowQueryRateTrueNoPoints(t *testing.T) {
//
//	payload := `{
//			"start": 1348452800000,
//			"end": 1348512200000,
//			"showTSUIDs": true,
//			"queries": [{
//				"metric": "ts01tsdb",
//				"rate": true,
//				"aggregator": "sum",
//				"tags": {
//					"host": "test"
//				}
//			}],
//			"showQuery": true
//		}`
//
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	assert.Equal(t, 200, code)
//	assert.Exactly(t, "[]", string(response))
//
//}

func TestTsdbQueryInvalid(t *testing.T) {

	cases := map[string]struct {
		payload string
		msg     string
	}{
		"TagsInvalidTagKey": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts12-_/.%&#;tsdb",
					"rate": "false",
					"rateOptions": {
						"counter": true,
						"counterMax": 15000
					},
					"aggregator": "sum",
					"tags": {
						"": "test-_/.%&#;1"
					}
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool",
		},
		"TagsInvalidTagValue": {
			`{
					"start": 1448452740000,
					"end": 1448458150000,
					"showTSUIDs": true,
					"queries": [{
						"metric": "ts12-_/.%&#;tsdb",
						"rate": "false",
						"rateOptions": {
							"counter": true,
							"counterMax": 15000
						},
						"aggregator": "sum",
						"tags": {
							"hos-_/.%&#;t": ""
						}
					}]
				}`,
			"json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool",
		},
		"TagsInvalidMetric": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "",
					"rate": "false",
					"rateOptions": {
						"counter": true,
						"counterMax": 15000
					},
					"aggregator": "sum",
					"tags": {
						"host": "test"
					}
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool",
		},
		"InvalidOrderOperation": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["downsample","aggregation","rate","test"]
				}]
			}`,
			"invalid operations in order array [test]",
		},
		"InvalidOrderDownsampleMissing": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["aggregation","rate"]
				}]
			}`,
			"downsample configured but no downsample found in order array",
		},
		"InvalidOrderMergeMissing": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["downsample","rate"]
				}]
			}`,
			"aggregation configured but no aggregation found in order array",
		},
		"InvalidOrderRateMissing": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["downsample","aggregation"]
				}]
			}`,
			"rate configured but no rate found in order array",
		},
		"InvalidOrderFilterValueMissing": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"filterValue":"==1",
					"downsample": "3m-avg",
					"aggregator": "sum",
					"order":["downsample","aggregation"]
				}]
			}`,
			"filterValue configured but no filterValue found in order array",
		},
		"InvalidOrderDuplicatedDownsample": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["aggregation","downsample","downsample","rate"]
				}]
			}`,
			"more than one downsample found in order array",
		},
		"InvalidOrderDuplicatedFilterValue": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"filterValue":"==1",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["aggregation","downsample","filterValue","filterValue","rate"]
				}]
			}`,
			"more than one filterValue found in order array",
		},
		"InvalidOrderDuplicatedMerge": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["aggregation","aggregation","downsample","rate"]
				}]
			}`,
			"more than one aggregation found in order array",
		},
		"InvalidOrderDuplicatedRate": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":["downsample","aggregation","rate","rate"]
				}]
			}`,
			"more than one rate found in order array",
		},
		"EmptyOrderOption": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts07_2tsdb",
					"rate": true,
					"downsample": "3m-avg",
					"rateOptions": {
						"counter": false
					},
					"aggregator": "sum",
					"order":[""]
				}]
			}`,
			"aggregation configured but no aggregation found in order array",
		},
		"InvalidRate": {
			`{
			"start": 1448452740000,
			"end": 1448458150000,
			"showTSUIDs": true,
			"queries": [{
				"metric": "ts12-_/.%&#;tsdb",
				"rate": "a",
				"rateOptions": {
					"counter": true,
					"counterMax": 15000
				},
				"aggregator": "sum",
				"tags": {
					"hos-_/.%&#;t": "test-_/.%&#;1"
				}
			}]
		}`,
			"json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool",
		},
		"InvalidRateOptionCounter": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts12-_/.%&#;tsdb",
					"rate": true,
					"rateOptions": {
						"counter": "a",
						"counterMax": 15000
					},
					"aggregator": "sum",
					"tags": {
						"hos-_/.%&#;t": "test-_/.%&#;1"
					}
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.counter of type bool",
		},
		"InvalidRateOptionCounterMax": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts12-_/.%&#;tsdb",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": "a"
					},
					"aggregator": "sum",
					"tags": {
						"hos-_/.%&#;t": "test-_/.%&#;1"
					}
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.counterMax of type int64",
		},
		"InvalidRateOptionCounterMaxNegative": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts12-_/.%&#;tsdb",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": -1
					},
					"aggregator": "sum",
					"tags": {
						"hos-_/.%&#;t": "test-_/.%&#;1"
					}
				}]
			}`,
			"counter max needs to be a positive integer",
		},
		"InvalidRateOptionResetValue": {
			`{
				"start": 1448452740000,
				"end": 1448458150000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts12-_/.%&#;tsdb",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"resetValue": "a"
					},
					"aggregator": "sum",
					"tags": {
						"hos-_/.%&#;t": "test-_/.%&#;1"
					}
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.resetValue of type int64",
		},
		"InvalidFilterWildcard2": {
			`{
				"start": 1448452740000,
				"end": 1448453940000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts13tsdb",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "host1|host2",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field filter: host1|host2",
		},
		"InvalidFilterLiteralOr1": {
			`{
				"start": 1448452740000,
				"end": 1448453940000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts13tsdb",
					"aggregator": "sum",
					"filters": [{
						"type": "literal_or",
						"tagk": "host",
						"filter": "*",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field filter: *",
		},
		"InvalidFilterNotLiteralOr1": {
			`{
				"start": 1448452740000,
				"end": 1448453940000,
				"showTSUIDs": true,
				"queries": [{
					"metric": "ts13tsdb",
					"aggregator": "sum",
					"filters": [{
						"type": "not_literal_or",
						"tagk": "host",
						"filter": "*",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field filter: *",
		},
	}

	for test, data := range cases {

		path := fmt.Sprintf("keyspaces/%s/api/query", ksMycenae)
		code, response, err := mycenaeTools.HTTP.POST(path, []byte(data.payload))
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		payloadError := tools.Error{}

		err = json.Unmarshal(response, &payloadError)
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		assert.Equal(t, 400, code, test)
		assert.Equal(t, data.msg, payloadError.Error, test)
	}
}
