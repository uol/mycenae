package main

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var ts10ID string

var hashMapPV2 map[string]string

func sendPointsV2(keyspace string) {

	fmt.Println("Setting up pointsV2_test.go tests...")

	tsPointsV2(keyspace)
	ts10(keyspace)
}

func tsPointsV2(keyspace string) {

	hashMapPV2 = map[string]string{}
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
		"ts1":   {"ts1", "test", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_1": {"ts01_1", "test", 1448452800, 60, 0.0, 1.0, 100},
		"ts1_2": {"ts01_2", "test", 1448452830, 60, 0.0, 1.0, 100},
		"ts1_3": {"ts01_3", "test", 1514808060, 604800, 1.0, 1.0, 100},
		// serie: 0,1,2,1,2,3,2,3,4...
		"ts2":   {"ts02", "test", 1448452800, 180, 0.0, 1.0, 30},
		"ts2_2": {"ts02", "test", 1448452860, 180, 1.0, 1.0, 30},
		"ts2_3": {"ts02", "test", 1448452920, 180, 2.0, 1.0, 30},
		// serie: 0,5,10,15,20...
		"ts3": {"ts03", "test", 1448452800, 1800, 0.0, 5, 480},
		// serie: 0.5,1.0,1.5,2.0...
		"ts4": {"ts04", "test", 1448452800, 604800, 0.0, 0.5, 208},
		// serie: 0, 2, 4, 6, 8...
		"ts5": {"ts05", "test", 1448452800, 120, 0.0, 2.0, 45},
		// serie: 1, 3, 5, 7, 9...
		"ts6": {"ts06", "test", 1448452860, 120, 1.0, 2.0, 45},
		// serie: 0, 1, 2, 3, 4...
		"ts7": {"ts07", "test", 1448452800, 60, 0.0, 1.0, 90},
		// serie: 0, 5, 10, 15, 20...
		"ts8": {"ts08", "test", 1448452800, 60, 0.0, 5.0, 90},
		// serie: 0, 1, 2, 3,...,14,15,75,76,77,...,88,89.
		"ts9":     {"ts09", "test", 1448452800, 60, 0.0, 1.0, 15},
		"ts9_2":   {"ts09", "test", 1448457300, 60, 75, 1.0, 15},
		"ts9_1":   {"ts09_1", "test", 1448452800, 60, 0.0, 1.0, 15},
		"ts9_1_2": {"ts09_1", "test", 1448457300, 60, 75, 1.0, 15},
		"ts11":    {"ts11", "test3", 1448452800, 60, 0.0, 1.0, 25},
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

		jsonPoints, err := json.Marshal(Points)
		if err != nil {
			log.Fatal(test, err, jsonPoints)
		}

		code, _, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
		if code != 204 {
			log.Fatal(test, code)
		}

		hashMapPV2[test] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(data.metric, map[string]string{"host": data.tagValue})
	}
}

func ts10(keyspace string) {

	metric := "t/s._-%10"
	tagKey := "hos-_/.%t"
	tagKey2 := "app"
	tagValue := "test-_/.%1"
	tagValue2 := "app1"
	startTime := 1448452800
	value := 0.0
	value2 := 0.0
	const numTotal int = 75
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
			tagKey:  tagValue,
			tagKey2: tagValue2,
		}
		Points[i].Timestamp = int64(startTime)
		i++
		value2++

		Points[i].Value = float32(value2)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid":  keyspace,
			tagKey:  tagValue,
			tagKey2: tagValue2,
		}
		Points[i].Timestamp = int64(startTime)

		startTime += 60
		value++
		value2++
	}

	jsonPoints, err := json.Marshal(Points)
	if err != nil {
		log.Fatal(err, jsonPoints)
	}

	code, _, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code != 204 {
		log.Fatal(code)
	}

	ts10ID = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
}

func postPointsAndCheck(t *testing.T, payload, id string, code, count, total, ts int) tools.MycenaePoints {

	path := fmt.Sprintf("keyspaces/%s/points", ksMycenae)
	returnCode, response, err := mycenaeTools.HTTP.POST(path, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, code, returnCode)
	assert.Equal(t, count, payloadPoints.Payload[id].Points.Count)
	assert.Equal(t, total, payloadPoints.Payload[id].Points.Total)
	assert.Equal(t, ts, len(payloadPoints.Payload[id].Points.Ts))

	return payloadPoints
}

func TestPointsV2PointsLimitTrueLessaThanLimit(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 150
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts1"], 200, 100, 100, 100)

	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts1"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += time.Minute.Seconds() * 1000
	}
}

/*
func TestPointsV2LimitFalseFullYear(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": false,
			"totalPoints": 150
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts1_3"] + `"
		}],
		"start": 1514808060000,
		"end": 1546257720000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts1_3"], 200, 53, 53, 53)

	i := 1.0
	dateStart := 1514808060000.0
	for _, value := range payloadPoints.ID[hashMapPV2["ts01_3"]].Data.Ts {

		assert.Exactly(t, i, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 604800000
	}
}
*/
func TestPointsV2LimitTrueBiggerLimitLower(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 90
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts1"], 200, 100, 100, 100)

	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts1"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++

		dateStart += time.Minute.Seconds() * 1000
		dateStart += time.Minute.Seconds() * 1000
	}
}

func TestPointsV2LimitTrueBiggerLimitUpper(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 50
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts1"], 200, 50, 100, 50)

	i := 0.5
	dateStart := 1448452830000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts1"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i += 2

		assert.Exactly(t, dateStart, value[0])
		dateStart += 120000
	}
}

func TestPointsV2LimitTrueNoPoints(t *testing.T) {
	t.Parallel()

	payload := `{
		"keys": [{
			"tsid":"` + hashMapPV2["ts1"] + `"
		}],
		"start": 1248452000000,
		"end": 1348452800000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 204, code)
	assert.Empty(t, string(response))
}

func TestPointsV2LimitTrueAproxMedia(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452440000,
		"end": 1448458381000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 34, payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Count)
	assert.Equal(t, 90, payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Total)
	assert.Equal(t, 34, len(payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts))
	i := 0
	dateStart := 1448452440000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		if value[0].(float64) < 1448452800000.0 {
			assert.Exactly(t, nil, value[1], payload)

		} else if value[0].(float64) > 1448458020000.0 {
			assert.Exactly(t, nil, value[1], payload)

		} else {
			media := float64((i + i + 1 + i + 2) / 3)
			assert.Exactly(t, media, value[1].(float64), payload)
			i++
		}
		assert.Exactly(t, dateStart, value[0].(float64), payload)
		dateStart += 180000

	}
}

func TestPointsV2LimitTrueAproxMediaExactBeginAndEnd(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)
	i := 0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		media := float64((i + i + 1 + i + 2) / 3)
		assert.Exactly(t, media, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueAproxMin(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"min",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452440000,
		"end": 1448458381000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 34, payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Count)
	assert.Equal(t, 90, payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Total)
	assert.Equal(t, 34, len(payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts))
	i := 0.0
	dateStart := 1448452440000.0

	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		if value[0].(float64) < 1448452800000.0 {
			assert.Exactly(t, nil, value[1])

		} else if value[0].(float64) > 1448458020000.0 {
			assert.Exactly(t, nil, value[1])

		} else {
			assert.Exactly(t, i, value[1].(float64))
			i++
		}
		assert.Exactly(t, dateStart, value[0].(float64))
		dateStart += 180000

	}
}

func TestPointsV2LimitTrueAproxMinExactBeginAndEnd(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"min",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)
	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueAproxMaxExactBeginAndEnd(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)
	i := 2.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueAproxSumExactBeginAndEnd(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)
	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		sum := float64(i + i + 1 + i + 2)
		assert.Exactly(t, sum, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueAproxPnt(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)

	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, 3.0, value[1])

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000

	}
}

func TestPointsV2LimitTrueAproxMaxHour(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "hour",
				"value": 2
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts3"] + `"
		}],
		"start": 1448452800000,
		"end": 1449316800000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 120, payloadPoints.Payload[hashMapPV2["ts3"]].Points.Count)
	assert.Equal(t, 480, payloadPoints.Payload[hashMapPV2["ts3"]].Points.Total)
	assert.Equal(t, 120, len(payloadPoints.Payload[hashMapPV2["ts3"]].Points.Ts))
	i := 15.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts3"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i += 20.0

		assert.Exactly(t, dateStart, value[0])
		dateStart += 7200000
	}

}

func TestPointsV2LimitTrueAproxMaxDay(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "day",
				"value": 2
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts3"] + `"
		}],
		"start": 1448452740000,
		"end": 1449316800000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts3"], 200, 6, 480, 6)
	i := 355.0
	dateStart := 1448409600000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts3"]].Points.Ts {

		if int64(value[0].(float64)) < 1449100800000 {

			assert.Exactly(t, i, value[1])
			i += 480.0

		} else {

			assert.Exactly(t, i, value[1])
			i += 120.0
		}

		assert.Exactly(t, dateStart, value[0])
		dateStart += 172800000
	}

}

func TestPointsV2LimitTrueAproxMaxWeek(t *testing.T) {
	t.Parallel()

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "week",
				"value": 4
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts4"] + `"
		}],
		"start": 1448452740000,
		"end": 1573646400000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts4"], 200, 52, 208, 52)
	i := 1.5

	//dateStart := 1447632000000.0
	dateStart := time.Date(2015, 11, 23, 0, 0, 0, 0, time.UTC)
	for _, value := range payloadPoints.Payload[hashMapPV2["ts4"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i += 2.0

		//assert.Exactly(t, dateStart, value[0])
		//dateStart += 172800000

		date := dateStart.Unix() * 1e+3

		assert.Exactly(t, float64(date), value[0])
		dateStart = dateStart.AddDate(0, 0, 28)

	}
}

func TestPointsV2LimitTrueAproxMaxMonth(t *testing.T) {
	t.Parallel()

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "month",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts4"] + `"
		}],
		"start": 1448452740000,
		"end": 1566734400000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts4"], 200, 16, 196, 16)
	i := 4.5
	dateStart := time.Date(2015, 11, 1, 0, 0, 0, 0, time.UTC)

	for _, value := range payloadPoints.Payload[hashMapPV2["ts4"]].Points.Ts {

		if int64(value[0].(float64)) <= 1501545600000 {

			assert.Exactly(t, i, value[1])
			i += 6.5

		} else if int64(value[0].(float64)) == 1509494400000 {

			i = 57.0
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) == 1517443200000 {

			i = 63.0
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) == 1525132800000 {

			i += 6.5
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) == 1533081600000 {

			i += 7.0
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) <= 1541030400000 {

			i += 6.5
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) == 1548979200000 {

			i += 6.0
			assert.Exactly(t, i, value[1])

		} else if int64(value[0].(float64)) == 1556668800000 {

			i += 7.0
			assert.Exactly(t, i, value[1])

		} else {
			i += 1.5
			assert.Exactly(t, i, value[1])
		}

		date := dateStart.Unix() * 1e+3

		assert.Exactly(t, float64(date), value[0])
		dateStart = dateStart.AddDate(0, 3, 0)

	}
}

func TestPointsV2LimitTrueAproxMaxYear(t *testing.T) {
	t.Parallel()

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "year",
				"value": 1
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts4"] + `"
		}],
		"start": 1448452740000,
		"end": 1545825600000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts4"], 200, 4, 162, 4)
	i := 2.5
	dateStart := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, value := range payloadPoints.Payload[hashMapPV2["ts4"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i += 26.0

		date := dateStart.Unix() * 1e+3
		assert.Exactly(t, float64(date), value[0])
		dateStart = dateStart.AddDate(1, 0, 0)
	}
}

func TestPointsV2LimitTrueAvgMenorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 25,
			"enabled": true,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)

	i := 1.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueAvgMaiorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 20,
			"enabled": true,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 15, 90, 15)

	i := 0.0
	dateStart := 1448452890000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		avg := (i+(i+1)+(i+2))/3 + i + 0.5

		assert.Exactly(t, avg, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 360000
	}
}

func TestPointsV2LimitTrueSumMenorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 25,
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)

	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		sum := i + (i + 1) + (i + 2)

		assert.Exactly(t, sum, value[1])
		i++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2LimitTrueSumMaiorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 20,
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 15, 90, 15)

	i := 0.0
	dateStart := 1448452890000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		sum := ((i + (i + 1) + (i + 2)) + (i + 1 + i + 2 + i + 3)) / 2

		assert.Exactly(t, sum, value[1])
		i += 2.0

		assert.Exactly(t, dateStart, value[0])
		dateStart += 360000
	}
}

func TestPointsV2LimitTruePntMenorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 25,
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 30, 90, 30)

	i := 3.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, i, value[1])

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000

	}

}

func TestPointsV2LimitTruePntMaiorLimit2(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 20,
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts2_3"] + `"
		}],
		"start": 1448452800000,
		"end": 1448458140000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts2_3"], 200, 15, 90, 15)

	i := 3.0
	dateStart := 1448452890000.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts2_3"]].Points.Ts {

		assert.Exactly(t, i, value[1])

		assert.Exactly(t, dateStart, value[0])
		dateStart += 360000
	}
}

func TestPointsV2MergeDateLimit(t *testing.T) {
	nameTS := "DateLimit"
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 100
		},
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts1"] + `"
				},{
					"tsid":"` + hashMapPV2["ts1_1"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458740000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 100, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Count)
	assert.Equal(t, 100, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Count)
	assert.Equal(t, 100, len(payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts))

	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, i+i, value[1])
		i++

		dateStart += time.Minute.Seconds() * 1000
		dateStart += time.Minute.Seconds() * 1000
	}
}

func TestPointsV2LimitTrueMergeMenorLimit2(t *testing.T) {
	nameTS := "MergeLimit<"

	payload := `{
			"downsample": {
			"pointLimit": true,
			"totalPoints": 70
				},
		 "merge": {
			"` + nameTS + `": {
				"option": "avg",
				"keys": [{
					"tsid":"` + hashMapPV2["ts5"] + `"
				},{
					"tsid":"` + hashMapPV2["ts6"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 90, 90)

	i := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++
	}
}

func TestPointsV2LimitTrueMergeMaiorLimit2(t *testing.T) {
	nameTS := "MergeLimit>"

	payload := `{
			"downsample": {
			"pointLimit": true,
			"totalPoints": 60
				},
		 "merge": {
			"` + nameTS + `": {
				"option": "avg",
				"keys": [{
					"tsid":"` + hashMapPV2["ts5"] + `"
				},{
					"tsid":"` + hashMapPV2["ts6"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 45, 90, 45)

	i := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {
		sum := (i + i + 1) / 2

		assert.Exactly(t, sum, value[1])
		i += 2
	}
}

func TestPointsV2LimitTrueMergeBiggerPointsSameDate(t *testing.T) {
	nameTS := "MergeMultiplePoints"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts5"] + `"
				},{
					"tsid":"` + hashMapPV2["ts6"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 180, 90)

	i := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, i*2, value[1])
		i++
	}
}

func TestPointsV2MergeAvg(t *testing.T) {
	nameTS := "MergeAvgMin"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "avg",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 180, 90)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		avg := (i + j) / 2
		assert.Exactly(t, avg, value[1])
		i++
		j += 5
	}
}

func TestPointsV2MergeMin(t *testing.T) {
	nameTS := "MergeMinMin"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "min",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 180, 90)

	i := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, i, value[1])
		i++
	}
}

func TestPointsV2MergeMax(t *testing.T) {
	nameTS := "MergeMaxMin"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "max",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 180, 90)

	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, j, value[1])
		j += 5
	}
}

func TestPointsV2MergeSum(t *testing.T) {
	nameTS := "MergeSumMin"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452740000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 180, 90)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		sum := i + j
		assert.Exactly(t, sum, value[1])
		i++
		j += 5
	}
}

func TestPointsV2MergeSumEnabledAvg(t *testing.T) {
	nameTS := "MergeAvgAndSumMin"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 180, 30)

	i := 0.0
	j := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum := ((i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)) / 3
		sum := (i+i+1+i+2)/3 + (j+j+5+j+10)/3

		assert.Exactly(t, sum, value[1])
		i += 3
		j += 15

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2MergeSumEnabledSum(t *testing.T) {
	nameTS := "MergeSumAndSumMin"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 180, 30)

	i := 0.0
	j := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum := ((i + j) + (i + 1 + j + 5) + (i + 2 + j + 10))
		sum := (i + i + 1 + i + 2) + (j + j + 5 + j + 10)
		assert.Exactly(t, sum, value[1])
		i += 3
		j += 15

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2MergeSumEnabledPnt(t *testing.T) {
	nameTS := "MergeSumAndPntMin"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458080000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 178, 30)

	dateStart := 1448452800000.0
	i := 6.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if int64(value[0].(float64)) < 1448458020000 {

			assert.Exactly(t, i, value[1])

		} else {

			assert.Exactly(t, 4.0, value[1])

		}

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2MergeSumEnabledPntHour(t *testing.T) {
	nameTS := "MergeSumAndPntHour"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "hour",
				"value": 1
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 2, 180, 2)

	sum := 120.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, sum, value[1])
		sum -= 60

		assert.Exactly(t, dateStart, value[0])
		dateStart += 3600000
	}
}

func TestPointsV2MergeSumEnabledAvgMinAproxMin(t *testing.T) {
	nameTS := "MergeSumAndAproxAvgMin"

	payload := `{
		"downsample": {
			"enabled": true,
			"pointLimit": true,
			"totalPoints": 25,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 180, 30)

	i := 0.0
	j := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum := ((i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)) / 3
		sum := (i+i+1+i+2)/3 + (j+j+5+j+10)/3
		assert.Exactly(t, sum, value[1])
		i += 3
		j += 15

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2MergeSumEnabledAvgMinAproxMax(t *testing.T) {
	nameTS := "MergeSumAndAproxAvgMax"

	payload := `{
		"downsample": {
			"enabled": true,
			"pointLimit": true,
			"totalPoints": 20,
			"options": {
				"approximation":"avg",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 15, 180, 15)

	i := 0.0
	j := 0.0
	dateStart := 1448452890000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum1 := ((i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)) / 3
		sum1 := (i+i+1+i+2)/3 + (j+j+5+j+10)/3
		i += 3
		j += 15
		//sum2 := ((i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)) / 3
		sum2 := (i+i+1+i+2)/3 + (j+j+5+j+10)/3
		i += 3
		j += 15

		sum := (sum1 + sum2) / 2
		assert.Exactly(t, sum, value[1])

		assert.Exactly(t, dateStart, value[0])
		dateStart += 360000

	}
}

func TestPointsV2MergeSumEnabledSumMinAproxMin(t *testing.T) {
	nameTS := "MergeSumAndAproxSumMin"

	payload := `{
		"downsample": {
			"enabled": true,
			"pointLimit": true,
			"totalPoints": 25,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 180, 30)

	i := 0.0
	j := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum := (i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)
		sum := (i + i + 1 + i + 2) + (j + j + 5 + j + 10)
		assert.Exactly(t, sum, value[1])
		i += 3
		j += 15

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2MergeSumEnabledSumMinAproxMax(t *testing.T) {
	nameTS := "MergeSumAndAproxSumMax"

	payload := `{
		"downsample": {
			"enabled": true,
			"pointLimit": true,
			"totalPoints": 20,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts8"] + `"
				},{
					"tsid":"` + hashMapPV2["ts7"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 15, 180, 15)

	i := 0.0
	j := 0.0
	dateStart := 1448452890000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		//sum1 := (i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)
		sum1 := (i + i + 1 + i + 2) + (j + j + 5 + j + 10)
		i += 3
		j += 15
		//sum2 := (i + j) + (i + 1 + j + 5) + (i + 2 + j + 10)
		sum2 := (i + i + 1 + i + 2) + (j + j + 5 + j + 10)
		i += 3
		j += 15

		sum := (sum1 + sum2) / 2
		assert.Exactly(t, sum, value[1])

		assert.Exactly(t, dateStart, value[0])
		dateStart += 360000

	}
}

func TestPointsV2MergeTimeDiff(t *testing.T) {
	nameTS := "TestMergeTimeDiff"
	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts1"] + `"
				},{
					"tsid":"` + hashMapPV2["ts1_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458770000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 200, 200, 200)

	i := 0.0
	dateStart := 1448452800000.0
	count := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		assert.Exactly(t, i, value[1])
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, dateStart, value[0])
		dateStart += 30000
	}
}

func TestPointsV2MergeTimeDiffDownsampleExactBeginAndEnd(t *testing.T) {
	nameTS := "TestMergeTimeDiff"
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"pnt",
				"unit": "min",
				"value": 3
			}
		},
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts1"] + `"
				},{
					"tsid":"` + hashMapPV2["ts1_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458770000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 34, 200, 34)

	i := 6.0
	dateStart := 1448452800000.0
	count := 0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if int64(value[0].(float64)) < 1448458740000 {

			assert.Exactly(t, i, value[1])

		} else {

			assert.Exactly(t, 2.0, value[1])

		}

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
		count++
	}
}

func TestPointsV2MergeTimeDiffDownsample(t *testing.T) {
	nameTS := "TestMergeTimeDiff"
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"min",
				"unit": "min",
				"value": 3
			}
		},
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts1"] + `"
				},{
					"tsid":"` + hashMapPV2["ts1_2"] + `"
				}]
			}
		},
		"start": 1448452801000,
		"end": 1448458740000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 34, 198, 34)

	i := 0.0
	dateStart := 1448452800000.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if int64(value[0].(float64)) == 1448452800000 {
			assert.Exactly(t, 1.0, value[1])
			i += 3

		} else if int64(value[0].(float64)) == 1448458740000 {
			assert.Exactly(t, 99.0, value[1])
			i += 3

		} else {
			assert.Exactly(t, i+i, value[1])
			i += 3
		}

		assert.Exactly(t, dateStart, value[0])
		dateStart += 180000
	}
}

func TestPointsV2NullValuesEnabled3Min(t *testing.T) {
	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		"keys": [{
			"tsid":"` + hashMapPV2["ts9_2"] + `"
		}],
		 "start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, hashMapPV2["ts9_2"], 200, 30, 30, 30)

	i := 0.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts9_2"]].Points.Ts {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)

			assert.Exactly(t, sum, value[1])
		} else {
			assert.Empty(t, value[1], "they should be nil")
		}
		i += 3

	}
}

func TestPointsV2NullValueMerge(t *testing.T) {
	nameTS := "MergeNullValue"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts7"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 90, 120, 90)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if i < 15 || i >= 75 {
			sum := i + j

			assert.Exactly(t, sum, value[1])
		} else {
			assert.Exactly(t, j, value[1])
		}
		i++
		j++

	}
}

func TestPointsV2BothValuesNullMerge(t *testing.T) {
	nameTS := "MergeNullValue"

	payload := `{
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_1_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 60, 30)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if i < 15 {
			sum := i + j
			assert.Exactly(t, sum, value[1])

		} else if i == 15 {
			i = 75.0
			j = 75.0
			sum := i + j
			assert.Exactly(t, sum, value[1])

		} else {
			sum := i + j
			assert.Exactly(t, sum, value[1])
		}
		i++
		j++

	}
}

func TestPointsV2BothValuesNullMergeAndDownsample(t *testing.T) {
	nameTS := "MergeNullValue"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"max",
				"unit": "min",
				"value": 3
			}
		},
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_1_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Equal(t, 30, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Count)
	assert.Equal(t, 60, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Total)
	assert.Equal(t, 30, len(payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts))

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, value[1])

		} else {
			assert.Empty(t, value[1], "they should be nil")

		}
		i += 3
		j += 3
	}
}

func TestPointsV2NullValueMergeEnabled3Min(t *testing.T) {
	nameTS := "MergeNullValueEnabled"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		 "merge": {
			"` + nameTS + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts7"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS+"]", 200, 30, 120, 30)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)

			assert.Exactly(t, sum, value[1])
		} else {
			sum := i + i + 1 + i + 2
			assert.Exactly(t, sum, value[1])
		}
		i += 3
		j += 3

	}
}

func TestPointsV22Merge(t *testing.T) {
	nameTS1 := "MergeNullValueEnabledSum"
	nameTS2 := "MergeNullValueEnabledAvg"

	payload := `{
		"downsample": {
			"enabled": true,
			"options": {
				"approximation":"sum",
				"unit": "min",
				"value": 3
			}
		},
		 "merge": {
			"` + nameTS1 + `": {
				"option": "sum",
				"keys": [{
					"tsid":"` + hashMapPV2["ts7"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				}]
			},
			"` + nameTS2 + `": {
				"option": "avg",
				"keys": [{
					"tsid":"` + hashMapPV2["ts7"] + `"
				},{
					"tsid":"` + hashMapPV2["ts9_2"] + `"
				}]
			}
		},
		"start": 1448452800000,
		"end": 1448458150000
	}`

	payloadPoints := postPointsAndCheck(t, payload, ksMycenae+"|merged:["+nameTS1+"]", 200, 30, 120, 30)

	i := 0.0
	j := 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS1+"]"].Points.Ts {

		if i < 15 || i >= 75 {
			//sum := i + j
			//assert.Exactly(t, sum, value[1])
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, value[1])

		} else {
			//assert.Exactly(t, j, value[1])
			sum := i + i + 1 + i + 2
			assert.Exactly(t, sum, value[1])
		}
		i += 3
		j += 3

	}

	assert.Equal(t, 30, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS2+"]"].Points.Count)
	assert.Equal(t, 120, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS2+"]"].Points.Total)
	assert.Equal(t, 30, len(payloadPoints.Payload[ksMycenae+"|merged:["+nameTS2+"]"].Points.Ts))

	i = 0.0
	j = 0.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS2+"]"].Points.Ts {

		if i < 15 || i >= 75 {
			//sum := i + j
			//assert.Exactly(t, sum, value[1])

			sum := ((i + i + 1 + i + 2) + (j + j + 1 + j + 2)) / 2
			assert.Exactly(t, sum, value[1])

		} else {
			sum := i + i + 1 + i + 2
			assert.Exactly(t, sum, value[1])
		}
		i += 3
		j += 3

	}
}

func TestPointsV2MoreThanOneTS(t *testing.T) {
	t.Parallel()
	
	payload := `{
		"keys": [{"tsid":"` + hashMapPV2["ts1"] + `"},{"tsid":"` + ts10ID + `"}
		],
		"start": 1448452800000,
		"end": 1548452700000
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/points", []byte(payload))
	assert.Equal(t, 200, code)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 100, payloadPoints.Payload[hashMapPV2["ts1"]].Points.Count)
	assert.Equal(t, 100, payloadPoints.Payload[hashMapPV2["ts1"]].Points.Total)
	assert.Equal(t, 100, len(payloadPoints.Payload[hashMapPV2["ts1"]].Points.Ts))

	dateStart := 1448452800000.0
	count := 0.0
	for _, value := range payloadPoints.Payload[hashMapPV2["ts1"]].Points.Ts {

		assert.Exactly(t, count, value[1])
		count++

		dateStart += time.Minute.Seconds() * 1000
		dateStart += time.Minute.Seconds() * 1000
	}

	assert.Equal(t, 25, payloadPoints.Payload[ts10ID].Points.Count)
	assert.Equal(t, 25, payloadPoints.Payload[ts10ID].Points.Total)
	assert.Equal(t, 25, len(payloadPoints.Payload[ts10ID].Points.Ts))

	dateStart = 1448452800000.0
	count = 0.0
	for _, value := range payloadPoints.Payload[ts10ID].Points.Ts {

		assert.Exactly(t, count, value[1])
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart += 60000
	}
}
