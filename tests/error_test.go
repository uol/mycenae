package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func postAndCheckError(test, path string, data interface{}, status int, t *testing.T) {

	body, _ := json.Marshal(data)
	code, content, err := mycenaeTools.HTTP.POST(path, body)
	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae), test)
	assert.Equal(t, status, code, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae), test)

	if status == 200 {
		var records tools.ResponseMetricTags

		err = json.Unmarshal(content, &records)
		assert.Equal(t, 1, records.TotalRecords, fmt.Sprintf("The request to get error list on keyspace %v did not return the array expected", ksMycenae), test)
		assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error list on keyspace %v", ksMycenae), test)
	}
}

func getErrorPayload(metric string) (string, string, string) {

	random := rand.Int()
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)

	return fmt.Sprintf(
			`{"metric": "%s", "tags": {"ksid": "%s", "%s": "%s"}, "timestamp": %d}`,
			metric, ksMycenae, tagKey, tagValue, time.Now().Unix(),
		),
		tagKey,
		tagValue
}

func getErrorPayloadTwoTags(metric string) (string, string, string, string, string) {

	random := rand.Int()
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)

	return fmt.Sprintf(
			`{"metric": "%s", "tags": {"ksid": "%s", "%s1": "%s1", "%s2": "%s2"}, "timestamp": %d}`,
			metric, ksMycenae, tagKey, tagValue, tagKey, tagValue, time.Now().Unix(),
		),
		fmt.Sprint(tagKey, "1"),
		fmt.Sprint(tagValue, "1"),
		fmt.Sprint(tagKey, "2"),
		fmt.Sprint(tagValue, "1")
}

/*
func TestGetErrorTimeseries(t *testing.T) {
	t.Parallel()
	metric := "TestGetTimeseriesV2Error"

	payload, tk, tv := getErrorPayload(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tk: tv})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)
	code, content, err := mycenaeTools.HTTP.GET(path)
	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tools.TsError{}
	err = json.Unmarshal(content, &data)
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))

	assert.Equal(t, 1, len(data), fmt.Sprintf("The result is different than expected"))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, "Wrong Format: Field \"value\" is required. NO information will be saved", data[0].Error)
	assert.Equal(t, payload, data[0].Message)
}

func TestGetErrorTimeseriesMultiple(t *testing.T) {
	t.Parallel()

	metric := "metric"
	tagKey := "testTagKey"
	tagValue := "testTagValue?"
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)
	timestamp2 := time.Now().Unix()

	payload = fmt.Sprintf(
		`{"value": 1.0, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, content, err := mycenaeTools.HTTP.GET(path)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tools.TsError{}

	err = json.Unmarshal(content, &data)
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))

	message2 := fmt.Sprintf(`{"value": 1.0, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2)

	assert.Condition(t, func() bool { return len(data) == 1 }, "The result is different than expected")
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Contains(t, data[0].Error, "Wrong Format: Tag value (testTagValue?) is not well formed. NO information will be saved")
	assert.Equal(t, message2, data[0].Message)

}

func TestGetErrorTimeseriesSame(t *testing.T) {
	t.Parallel()

	metric := "metric"
	tagKey := "testTagKey"
	tagValue := "testTagValue"
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)
	timestamp2 := time.Now().Unix()

	payload = fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, content, err := mycenaeTools.HTTP.GET(path)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tools.TsError{}

	err = json.Unmarshal(content, &data)

	message := fmt.Sprintf(`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2)

	assert.Equal(t, 1, len(data), fmt.Sprintf("The result is different than expected"))
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, "Wrong Format: Field \"value\" is required. NO information will be saved", data[0].Error)
	assert.Equal(t, message, data[0].Message)

}

func TestGetErrorsTimeseries(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		hashID string
		ksid   string
		status int
	}{
		"NoContent": {
			"notExists",
			ksMycenae,
			204,
		},
		"KeyspaceNotExists": {
			"notExists",
			"notExists",
			404,
		},
		"EmptyKeyspace": {
			"notExists",
			"",
			404,
		},
		"EmptyErrorId": {
			"",
			ksMycenae,
			404,
		},
	}

	for test, data := range cases {

		path := fmt.Sprintf("keyspaces/%s/errors/%s", data.ksid, data.hashID)
		code, _, err := mycenaeTools.HTTP.GET(path)

		assert.NoError(t, err, "There was an error with request to get error %v on keyspace %v and test %s", data.hashID, data.ksid, test)
		assert.Equal(t, data.status, code, "The request to get error %v on keyspace %v did not return the expected http code. Test: %s", data.hashID, data.ksid, test)
	}
}

func TestGetErrorPostTimeseries(t *testing.T) {
	t.Parallel()

	random := rand.Int()
	metric := ""
	value := 5.0
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, _, err := mycenaeTools.HTTP.POST(path, nil)

	assert.NoError(t, err, "There was an error with request to get error %v on keyspace %v", hashID, ksMycenae)
	assert.Equal(t, 405, code, "The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae)

}

func TestListErrorTimeseriesIdsMultipleTags(t *testing.T) {
	t.Parallel()

	metric := "TestListTimeseriesV2ErrorIdsMultipleTags"

	payload, tk1, tv1, tk2, tv2 := getErrorPayloadTwoTags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: metric,
		Tags: []tools.TsTagV2{{TagKey: tk1, TagValue: tv1}, {TagKey: tk2, TagValue: tv2}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsMetric(t *testing.T) {
	t.Parallel()

	metric := fmt.Sprint("TestListTimeseriesV2ErrorIdsMetric-", time.Now().Unix())

	payload, _, _, _, _ := getErrorPayloadTwoTags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: metric,
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsTagKey(t *testing.T) {
	t.Parallel()

	payload, tk1, _, _, _ := getErrorPayloadTwoTags("TestListTimeseriesV2ErrorIdsTagKey")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Tags: []tools.TsTagV2{{TagKey: tk1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsTagValue(t *testing.T) {
	t.Parallel()

	payload, _, tv1, _, _ := getErrorPayloadTwoTags("TestListTimeseriesV2ErrorIdsTagValue")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Tags: []tools.TsTagV2{{TagValue: tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsTagKeyAndTagValue(t *testing.T) {
	t.Parallel()

	payload, tk1, tv1, _, _ := getErrorPayloadTwoTags("TestListTimeseriesV2ErrorIdsTagKeyAndTagValue")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Tags: []tools.TsTagV2{{TagKey: tk1, TagValue: tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsTagKeyAndMetric(t *testing.T) {
	t.Parallel()

	metric := "TestListTimeseriesV2ErrorIdsTagKeyAndMetric"

	payload, tk1, _, _, _ := getErrorPayloadTwoTags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: metric,
		Tags:   []tools.TsTagV2{{TagKey: tk1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsTagValueAndMetric(t *testing.T) {
	t.Parallel()

	metric := "TestListTimeseriesV2ErrorIdsTagValueAndMetric"

	payload, _, tv1, _, _ := getErrorPayloadTwoTags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: metric,
		Tags:   []tools.TsTagV2{{TagValue: tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListErrorTimeseriesIdsNoResults(t *testing.T) {
	t.Parallel()

	_, tk, tv := getErrorPayload("")

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: "metric.*",
		Tags:   []tools.TsTagV2{{TagKey: tk, TagValue: tv}},
	}

	postAndCheckError("", path, data, 204, t)
}

func TestListErrorTimeseriesIds(t *testing.T) {
	t.Parallel()
	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	metric := "metricTest"

	payload, tk, tv := getErrorPayload(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	cases := map[string]struct {
		body   tools.TsErrorV2
		status int
	}{
		"MetricRegexp": {
			tools.TsErrorV2{
				Metric: "metric.*",
				Tags:   []tools.TsTagV2{{TagKey: tk, TagValue: tv}},
			}, 200,
		},
		"WrongTagValue": {
			tools.TsErrorV2{
				Metric: metric,
				Tags:   []tools.TsTagV2{{TagKey: tk, TagValue: fmt.Sprint(tv, "1")}},
			}, 204,
		},
		"WrongTagKey": {
			tools.TsErrorV2{
				Metric: metric,
				Tags:   []tools.TsTagV2{{TagKey: fmt.Sprint(tk, "1"), TagValue: tv}},
			}, 204,
		},
		"WrongMetric": {
			tools.TsErrorV2{
				Metric: fmt.Sprint(metric, "1"),
				Tags:   []tools.TsTagV2{{TagKey: tk, TagValue: tv}},
			}, 204,
		},
	}

	for test, data := range cases {

		postAndCheckError(test, path, data.body, data.status, t)
	}
}

func TestGetErrorTimeseriesMultipleIdsKeyspaceNotFound(t *testing.T) {
	t.Parallel()

	data := tools.TsErrorV2{
		Metric: "metric.*",
		Tags:   []tools.TsTagV2{{}},
	}

	postAndCheckError("", "keyspaces/notExists/errortags", data, 404, t)
}

func TestGetErrorTimeseriesMultipleIdsParamsSizeAndFrom(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"SizeString":   "size=a",
		"NegativeSize": "size=-1",
		"SizeZero":     "size=0",
		"NegativeFrom": "from=-1",
	}

	for test, param := range cases {

		path := fmt.Sprintf("keyspaces/%s/errortags?%s", ksMycenae, param)
		data := tools.TsErrorV2{
			Metric: "metric.*",
			Tags:   []tools.TsTagV2{{}},
		}

		postAndCheckError(test, path, data, 400, t)
	}
}

func TestGetErrorTimeseriesMalformedPayload(t *testing.T) {
	t.Parallel()

	type tsErrorV2 struct {
		Metric int
		Tags   []tools.TsTagV2 `json:"tagsError"`
	}

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		1,
		[]tools.TsTagV2{{TagKey: "tagKey", TagValue: "tagValue"}},
	}

	postAndCheckError("", path, data, 400, t)
}

func TestGetErrorTimeseriesFieldNotExists(t *testing.T) {
	t.Parallel()

	type tsErrorV2_1 struct {
		Test string `json:"metric1"`
	}

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2_1{
		"test",
	}

	body, _ := json.Marshal(data)

	code, _, err := mycenaeTools.HTTP.POST(path, body)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae))
	assert.True(t, code == 200 || code == 204, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae))

}

func TestGetErrorTimeseriesMethodNotAllowed(t *testing.T) {
	t.Parallel()

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tools.TsErrorV2{
		Metric: "metric",
		Tags: []tools.TsTagV2{{TagKey: "tagKey", TagValue: "tagValue"}},
	}

	body, _ := json.Marshal(data)

	code, _, err := mycenaeTools.HTTP.PUT(path, body)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae))
	assert.Equal(t, 405, code, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae))

}
*/
