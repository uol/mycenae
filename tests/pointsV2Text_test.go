package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var hashMapPV2T map[string]string

func tsPointsV2Text(keyspace string) {

	hashMapPV2T = map[string]string{}
	cases := map[string]struct {
		tags       map[string]string
		metric     string
		value      string
		startTime  int
		iStartTime int
		count      float64
		iCount     float64
		numTotal   int
	}{
		"tsText1": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test"},
			"ts01", "test", 1448452800, 60, 1.0, 1, 100,
		},
		"tsText1_1": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test"},
			"ts01_1", "newtest", 1448452830, 60, 1.0, 1, 100,
		},
		"tsText3": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test3"},
			"ts03", "test", 1448452800, 60, 1.0, 1, 25,
		},
		"tsText2": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test1"},
			"ts02", "test", 1448452800, 60, 1.0, 1, 25,
		},
		"tsText2_1": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test", "app": "app1"},
			"ts02", "test", 1448452800, 60, 1.0, 2, 25,
		},
		"tsText2_2": {
			map[string]string{"ksid": keyspace, "ttl": "1", "host": "test", "app": "app1"},
			"ts02", "test", 1448452800, 60, 2.0, 2, 25,
		},
	}

	for test, data := range cases {

		Points := make([]tools.TextPoint, data.numTotal)

		for i := 0; i < data.numTotal; i++ {
			Points[i].Text = fmt.Sprintf("%v%.1f", data.value, data.count)
			Points[i].Metric = data.metric
			Points[i].Tags = data.tags
			Points[i].Timestamp = int64(data.startTime)
			data.count += data.iCount
			data.startTime += 60
		}

		jsonPoints, err := json.Marshal(Points)
		if err != nil {
			log.Fatal(test, err, jsonPoints)
		}

		mycenaeTools.HTTP.POSTstring("api/text/put", string(jsonPoints))
		hashMapPV2T[test] = tools.GetTextHashFromMetricAndTags(data.metric, data.tags)

	}
}

/*
TS04
	interval: 1h
	total: 100 points
*/
func tsText4(keyspace string) bool {

	metric := "ts04txt"
	metric2 := "ts05"
	tagKey := "host"
	tagValue := "test"
	startTime := 1448452800

	var value string
	for i := 0; i < 10000; i++ {
		value += "W"
	}

	const numTotal int = 100
	Points := [numTotal]tools.TextPoint{}

	for i := 0; i < numTotal; i++ {
		Points[i].Text = value
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,
			"ttl":  "1",
			tagKey: tagValue,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Text = "!@#$%*()_+{}//;<b>.\\[]&<\b>"
		Points[i].Metric = metric2
		Points[i].Tags = map[string]string{
			"ksid": keyspace,
			"ttl":  "1",
			tagKey: tagValue,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 3600
	}

	jsonPoints, err := json.Marshal(Points)
	if err != nil {
		log.Fatal("tsText4", err, jsonPoints)
	}

	mycenaeTools.HTTP.POSTstring("api/text/put", string(jsonPoints))
	hashMapPV2T["tsText4"] = tools.GetTextHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, "ksid": keyspace, "ttl": "1"})
	hashMapPV2T["tsText4_1"] = tools.GetTextHashFromMetricAndTags(metric2, map[string]string{tagKey: tagValue, "ksid": keyspace, "ttl": "1"})

	return true
}

func tsText6(keyspace string) bool {

	metric := "ts06_1"
	tagKey := "host"
	tagValue := "test"
	startTime := 1448452800
	value := "test"
	count := 1.0
	const numTotal int = 100
	TextPoints := [numTotal]tools.TextPoint{}
	Points := [numTotal]tools.Point{}

	for i := 0; i < numTotal; i++ {
		TextPoints[i].Text = fmt.Sprintf("%v%.1f", value, count)
		TextPoints[i].Metric = metric
		TextPoints[i].Tags = map[string]string{
			"ksid": keyspace,
			"ttl":  "1",
			tagKey: tagValue,
		}
		TextPoints[i].Timestamp = int64(startTime)
		startTime += 60
		count++
	}

	jsonPoints, err := json.Marshal(TextPoints)
	if err != nil {
		log.Fatal("tsText6", err, jsonPoints)
	}

	mycenaeTools.HTTP.POSTstring("api/text/put", string(jsonPoints))
	hashMapPV2T["tsText6"] = tools.GetTextHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, "ksid": keyspace, "ttl": "1"})

	count = 1.0
	startTime = 1448452800
	for i := 0; i < numTotal; i++ {
		Points[i].Value = float32(count)
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,
			"ttl":  "1",
			tagKey: tagValue,
		}
		Points[i].Timestamp = int64(startTime)
		count++
		startTime += 60
	}

	jsonPoints, err = json.Marshal(Points)
	if err != nil {
		log.Fatal("tsText6b", err, jsonPoints)
	}

	mycenaeTools.HTTP.POST("api/put", jsonPoints)
	hashMapPV2T["tsText6_1"] = tools.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, "ksid": keyspace, "ttl": "1"})

	return true
}

func sendPointsV2Text(keySet string) {

	fmt.Println("Setting up pointsV2Text_test.go tests...")

	tsPointsV2Text(keySet)
	tsText4(keySet)
	tsText6(keySet)
}

func postPointsTextAndCheck(t *testing.T, payload, id string, code, count, total, ts int) tools.MycenaePointsText {

	path := fmt.Sprintf("keysets/%s/points", ksMycenae)

	returnCode, response, err := mycenaeTools.HTTP.POST(path, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, code, returnCode)

	payloadPoints := tools.MycenaePointsText{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, count, payloadPoints.Payload[hashMapPV2T[id]].Points.Count)
	assert.Equal(t, total, payloadPoints.Payload[hashMapPV2T[id]].Points.Total)
	assert.Equal(t, ts, len(payloadPoints.Payload[hashMapPV2T[id]].Points.Ts))

	return payloadPoints
}

func TestPointsV2TextLimitTrue(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 50
		},
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`

	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 100, 100, 100)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextLimitFalse(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": false
		},
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 100, 100, 100)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextWithoutDownsample(t *testing.T) {
	payload := `{
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 100, 100, 100)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextDateLimit(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": true,
			"totalPoints": 50
		},
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448453400000,
		"end": 1448454540000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 20, 20, 20)

	dateStart := 1448453400000.0
	count := 11.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextFirstPointsNull(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": false
		},
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448452790000,
		"end": 1448453340000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 10, 10, 10)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextLastPointsNull(t *testing.T) {
	payload := `{
		"downsample": {
			"pointLimit": false
		},
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448458200000,
		"end": 1448458840000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 10, 10, 10)

	dateStart := 1448458200000.0
	count := 91.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}
}

func TestPointsV2TextNoPointsUpper(t *testing.T) {
	payload := `{
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1448458741000,
		"end": 1448558740000
	}`
	code, response, err := mycenaeTools.HTTP.POST("keysets/"+ksMycenae+"/points", []byte(payload))

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, http.StatusNoContent, code)
	assert.Empty(t, string(response))
}

func TestPointsV2TextNoPointsBelow(t *testing.T) {
	payload := `{
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText1"] + `"
		}],
		"start": 1438458741000,
		"end": 1448452700000
	}`
	code, response, err := mycenaeTools.HTTP.POST("keysets/"+ksMycenae+"/points", []byte(payload))

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 204, code)
	assert.Empty(t, string(response))
}

func TestPointsV2TextMoreThanOneTS(t *testing.T) {
	payload := `{
		"text": [{
			"TsID":"` + hashMapPV2T["tsText1"] + `"
		},{
			"TsID":"` + hashMapPV2T["tsText2"] + `"
		}],
		"start": 1448452800000,
		"end": 1548452700000
	}`

	payloadPoints := postPointsTextAndCheck(t, payload, "tsText1", 200, 100, 100, 100)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText1"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}

	assert.Equal(t, 25, payloadPoints.Payload[hashMapPV2T["tsText2"]].Points.Count)
	assert.Equal(t, 25, payloadPoints.Payload[hashMapPV2T["tsText2"]].Points.Total)
	assert.Equal(t, 25, len(payloadPoints.Payload[hashMapPV2T["tsText2"]].Points.Ts))

	dateStart = 1448452800000.0
	count = 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText2"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + 60000
	}
}

func TestPointsV2TextValueLimit5000Chars(t *testing.T) {
	payload := `{
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText4"] + `"
		}],
		"start": 1448452800000,
		"end": 1448809200000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText4", 200, 50, 50, 50)

	dateStart := 1448452800000.0

	var textValue string
	for i := 0; i < 10000; i++ {
		textValue += "W"
	}

	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText4"]].Points.Ts {

		assert.Exactly(t, textValue, value[1].(string))

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + 3600000
	}

}

func TestPointsV2TextValueSpecialChars(t *testing.T) {
	payload := `{
		"text": [{
		   "TsID":"` + hashMapPV2T["tsText4_1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448809200000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText4_1", 200, 50, 50, 50)

	dateStart := 1448452800000.0

	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText4_1"]].Points.Ts {

		assert.Exactly(t, "!@#$%*()_+{}//;<b>.\\[]&<\b>", value[1].(string))

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + 3600000
	}

}

func TestPointsV2TextTSWithNumbersAndText(t *testing.T) {
	payload := `{
		"text": [{
			"TsID":"` + hashMapPV2T["tsText6"] + `"
		}],
		"keys": [{
			"TsID":"` + hashMapPV2T["tsText6_1"] + `"
		}],
		"start": 1448452800000,
		"end": 1448512200000
	}`
	payloadPoints := postPointsTextAndCheck(t, payload, "tsText6", 200, 100, 100, 100)

	dateStart := 1448452800000.0
	count := 1.0
	for _, value := range payloadPoints.Payload[hashMapPV2T["tsText6"]].Points.Ts {

		assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", count), value[1].(string))
		count++

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + time.Minute.Seconds()*1000
	}

}

func TestPointsV2TextMergeText(t *testing.T) {
	nameTS := "TestPointsV2MergeText"
	payload := `{
		"merge": {
			"` + nameTS + `": {
				"keys": [{
		   			"TsID":"` + hashMapPV2T["tsText1"] + `"
				},{
		   			"TsID":"` + hashMapPV2T["tsText1_1"] + `"
				}]
			}
  		},
		"start": 1148452800000,
  		"end": 1848458770000
	}`
	code, response, err := mycenaeTools.HTTP.POST("keysets/"+ksMycenae+"/points", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	payloadPoints := tools.MycenaePoints{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Count)
	assert.Equal(t, 200, payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Total)
	assert.Equal(t, 200, len(payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts))

	dateStart := 1448452800000.0
	count := 0
	i := 1.0
	for _, value := range payloadPoints.Payload[ksMycenae+"|merged:["+nameTS+"]"].Points.Ts {

		if count == 0 {
			assert.Exactly(t, fmt.Sprintf("%v%.1f", "test", i), value[1].(string))
			count = 1

		} else {
			assert.Exactly(t, fmt.Sprintf("%v%.1f", "newtest", i), value[1].(string))
			count = 0
			i++
		}

		assert.Exactly(t, dateStart, value[0])
		dateStart = dateStart + 30000
	}
}
