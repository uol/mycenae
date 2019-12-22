package main

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func assertElastic(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	lenTags := len(tags) - 1 // remove ksid

	meta := mycenaeTools.Solr.Timeseries.GetMeta(keyspace, hashID)
	assert.Equal(t, hashID, meta.ID, "meta id corresponding to the payload does not match the one in solr")
	assert.Equal(t, metric, meta.Metric, "metric sent in the payload does not match the one in solr")
	assert.Equal(t, lenTags, len(meta.Tags))
	for _, tag := range meta.Tags {
		value, ok := tags[tag.TagKey]
		assert.True(t, ok, fmt.Sprintf("tag key %s not found", tag.TagKey))
		assert.Equal(t, value, tag.TagValue)
	}
}

func assertElasticEmpty(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	count := mycenaeTools.Solr.Timeseries.GetMetricPost(keyspace, metric)
	assert.Equal(t, 0, count, fmt.Sprintf("metric sent in the payload does not match the one in solr: m=%s, tags=%+v, ks=%s", metric, tags, keyspace))

	for tagKey, tagValue := range tags {

		if tagKey == "ttl" {
			continue
		}

		count := mycenaeTools.Solr.Timeseries.GetTagKeyPost(keyspace, tagKey)
		assert.Equal(t, 0, count)

		count = mycenaeTools.Solr.Timeseries.GetTagValuePost(keyspace, tagValue)
		assert.Equal(t, 0, count)
	}

	meta := mycenaeTools.Solr.Timeseries.GetMeta(keyspace, hashID)
	assert.Nil(t, meta, "document has been found when it should not")
}

func assertMycenae(t *testing.T, keyspace string, start int64, end int64, value float32, hashID string) {

	status, response := mycenaeTools.Mycenae.GetPoints(keyspace, start, end, hashID)

	if assert.Equal(t, 200, status) {
		if assert.Equal(t, 1, len(response.Payload[hashID].Points.Ts)) {
			assert.True(t, start*1000 <= int64(response.Payload[hashID].Points.Ts[0][0].(float64)) && int64(response.Payload[hashID].Points.Ts[0][0].(float64)) <= end*1000)
			assert.Equal(t, value, float32(response.Payload[hashID].Points.Ts[0][1].(float64)))
		}
	}
}

func assertMycenaeEmpty(t *testing.T, keyspace string, start int64, end int64, hashID string) {

	status, response := mycenaeTools.Mycenae.GetPoints(keyspace, start, end, hashID)

	assert.Equal(t, 204, status)
	assert.Equal(t, tools.MycenaePoints{}, response)
}

func TestRESTv2PayloadWithAllFields(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil

	dateBefore := time.Now().Unix()

	sendRESTPayloadAndAssertPoint(t, p, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithMoreThanOneTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p.TagKey2 = fmt.Sprint("testTagKey2-", p.Random)
	p.TagValue2 = fmt.Sprint("testTagValue2-", p.Random)
	p.Tags[p.TagKey2] = p.TagValue2

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2MultiplePointsSameIDAndTimestampsGreaterThanDay(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	timestamps := [5]int64{
		*p.Timestamp,
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -1).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -2).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -3).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -4).Unix(),
	}

	values := [5]float32{
		10.0,
		2.2,
		3.3333,
		44444.444,
		0.555555,
	}

	for i := 0; i < 5; i++ {

		*p.Timestamp = timestamps[i]
		*p.Value = values[i]

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
		assert.Equal(t, http.StatusNoContent, statusCode)
	}

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	time.Sleep(tools.Sleep3)

	for i := 0; i < 5; i++ {
		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], values[i], hashID)
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2MultiplePointsSameIDAndNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil
	dateBefore := time.Now()

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 3; i++ {

		*p.Value = float32(i)

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
		assert.Equal(t, http.StatusNoContent, statusCode)

		time.Sleep(tools.Sleep3)

		dateAfter := time.Now()
		assertMycenae(t, ksMycenae, dateBefore.Unix(), dateAfter.Unix(), *p.Value, hashID)
		dateBefore = dateAfter
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2PayloadWithOnlyNumbersOrLetters(t *testing.T) {
	t.Parallel()

	numbers := "01234567890123456789"
	letters := "abcdefghijklmnopqrstuvwxyzabcd"

	numbersOrLetters := []string{numbers, letters}

	for _, numOrLetters := range numbersOrLetters {

		var wg sync.WaitGroup
		wg.Add(3)

		go testMetric(t, numOrLetters, &wg, false)
		go testTagKey(t, numOrLetters, &wg, false)
		go testTagValue(t, numOrLetters, &wg, false)

		wg.Wait()
	}
}

func TestRESTv2PayloadWithSpecialChars(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(tests))

	for test := range tests {

		go func(test int) {

			p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

			specialChars := fmt.Sprint("9Aa35ffg", p.Random, "...-___-.%&#;./.a1")

			switch test {
			case 0:
				p.Metric = specialChars
				*p.Value = float32(test)
			case 1:
				p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", specialChars: p.TagValue}
				*p.Value = float32(test)
			case 2:
				p.Tags[p.TagKey] = specialChars
				*p.Value = float32(test)
			}

			ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

			statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
			assert.Equal(t, http.StatusNoContent, statusCode)

			// special chars take longer to be saved
			time.Sleep(tools.Sleep3 * 2)

			hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

			assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

			assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2PayloadWithLastCharUnderscore(t *testing.T) {
	t.Parallel()

	lastCharUnderscore := fmt.Sprint("9Aa35ffg...-___-..._")

	var wg sync.WaitGroup
	wg.Add(3)

	go testMetric(t, lastCharUnderscore, &wg, false)
	go testTagKey(t, lastCharUnderscore, &wg, false)
	go testTagValue(t, lastCharUnderscore, &wg, false)

	wg.Wait()
}

func TestRESTv2PayloadWithNegativeValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = -5.0

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithZeroValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = 0.0

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithMaxFloat32Value(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = math.MaxFloat32

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadsWithSameMetricTagsTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

	*p.Value = 6.1

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadsWithSameMetricTagsTimestampTwoEqualTags(t *testing.T) {
	t.Parallel()

	value1 := 5.0
	value2 := 6.0
	metric, tagKey1, tagValue1, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()
	tagKey2 := tagKey1
	tagValue2 := tagValue1

	payload1 := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value1,
		metric,
		ksMycenae,
		tagKey1,
		tagValue1,
		tagKey2,
		tagValue2,
		timestamp,
	)

	payload2 := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value2,
		metric,
		ksMycenae,
		tagKey1,
		tagValue1,
		tagKey2,
		tagValue2,
		timestamp,
	)

	tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey1: tagValue1}

	hashID := tools.GetHashFromMetricAndTags(metric, tags)

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload1)
	assert.Equal(t, http.StatusNoContent, statusCode)

	statusCode, _ = mycenaeTools.HTTP.POSTstring("api/put", payload2)
	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)

	assertMycenae(t, ksMycenae, timestamp, timestamp, float32(value2), hashID)

	assertElastic(t, ksMycenae, metric, tags, hashID)
}

func TestRESTv2PayloadWithStringValue(t *testing.T) {
	t.Parallel()

	value := "testValue"
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"value": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
}

func TestRESTv2PayloadWithValueNotSent(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p.Value = nil

	errMessage := "Wrong Format: Field \"value\" is required."

	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithEmptyValues(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// empty value
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value":, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty metric
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric":, "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			1.0,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := ""
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty ksid
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid":, "%v": "%v"}, "timestamp": %v}]`,
			2.0,
			metric,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": "", "ttl": "1", tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag key
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", : "%v"}, "timestamp": %v}]`,
			3.0,
			metric,
			ksMycenae,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", "": tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag value
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v":}, "timestamp": %v}]`,
			4.0,
			metric,
			ksMycenae,
			tagKey,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: ""}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty timestamp
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp":}]`,
			5.0,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep3).Unix())

		wg.Done()
	}()
	wg.Wait()
}

func TestRESTv2PayloadWithInvalidChars(t *testing.T) {
	t.Parallel()

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	var wgOut sync.WaitGroup
	wgOut.Add(len(invalidChars))

	for _, invalidChar := range invalidChars {

		go func(char string) {

			var wgIn sync.WaitGroup
			wgIn.Add(3)

			go testInvalidMetric(t, char, &wgIn, false)
			go testInvalidTagKey(t, char, &wgIn, false)
			go testInvalidTagValue(t, char, &wgIn, false)

			wgIn.Wait()
			wgOut.Done()

		}(invalidChar)
	}
	wgOut.Wait()
}

func TestRESTv2PayloadWithInvalidCharsAtOnce(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	payload := []tools.Payload{}

	timestamp := time.Now().Unix()

	for _, invalidChar := range invalidChars {

		for test := range tests {

			p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			*p.Timestamp = timestamp

			switch test {
			case 0:
				p.Metric = invalidChar
			case 1:
				p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", invalidChar: p.TagValue}
			case 2:
				p.Tags[p.TagKey] = invalidChar
			}

			payload = append(payload, *p)
		}
	}

	ps := tools.PayloadSlice{PS: payload}

	statusCode, _, err := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, statusCode)
	time.Sleep(tools.Sleep3)

	/*var restError tools.RestErrors

	err = json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, len(payload), len(restError.Errors))
	assert.Equal(t, len(payload), restError.Failed)
	assert.Equal(t, 0, restError.Success)

	for i, err := range restError.Errors {

		assert.Equal(t, *payload[0].Value, restError.Errors[i].Datapoint.Value)
		assert.Equal(t, len(payload[0].Tags), len(restError.Errors[i].Datapoint.Tags))
		assert.Equal(t, payload[0].Tags["ksid"], restError.Errors[i].Datapoint.Tags["ksid"])
		assert.Contains(t, err.Error, "Wrong Format: ")
	}*/

	for _, p := range payload {

		hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

		assertMycenaeEmpty(t, p.Tags["ksid"], timestamp, timestamp, hashID)
		assertElasticEmpty(t, p.Tags["ksid"], p.Metric, p.Tags, hashID)
	}
}

func TestRESTv2PayloadWithAKsidTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, p.TagKey)

	errMessage := `At least one tag other than "ksid" is required.`
	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithoutKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, "ksid")

	errMessage := `Wrong Format: Tag "ksid" is required.`
	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), "", p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithInvalidKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags["ksid"] = "ksMycenae"

	errMessage := "Keyspace ksmycenae does not exist"

	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithInvalidTimestamp(t *testing.T) {
	t.Parallel()

	dateBefore := time.Now().Unix()

	value := 5.0
	metric, tagKey, tagValue, _ := mycenaeTools.Mycenae.GetRandomMetricTags()
	timestamp := 9999999.9

	payload := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithStringTimestamp(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": "%v"}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}, timestamp, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithBadFormatedJson(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}, timestamp, timestamp)
}

func TestRESTv2PayloadWithTwoPoints(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2PayloadWithTwoPointsWithHeader(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2PayloadWithTwoPointsWithTwoTagsEach(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2PayloadWithTwoPointsAndAWrongFormatEmptyString(t *testing.T) {
	t.Parallel()

	cases := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(cases))

	for test := range cases {

		go func(test int) {

			pInvalid := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			var errMessage string

			switch test {
			case 0:
				pInvalid.Metric = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Field \"metric\" has a invalid format."
			case 1:
				pInvalid.TagKey2 = ""
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag key has a invalid format."
			case 2:
				pInvalid.TagValue2 = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag value has a invalid format."
			}

			p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
			p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
			p2.Tags[p2.TagKey2] = p2.TagValue2

			ps := tools.PayloadSlice{PS: []tools.Payload{*pInvalid, *p2}}

			sendRESTPayloadWithMoreThanAPointAndAssertError(t, errMessage, ps, 0)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2EmptyPayload(t *testing.T) {
	t.Parallel()

	payload := fmt.Sprintf(`[]`)

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 204, statusCode)
}

func testMetric(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Metric = value

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testTagKey(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", value: p.TagValue}

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testTagValue(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidMetric(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Metric = value

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := "Wrong Format: Field \"metric\" has a invalid format."
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidTagKey(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", value: p.TagValue}

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := "Wrong Format: Tag key has a invalid format."
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidTagValue(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := "Wrong Format: Tag value has a invalid format."
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func sendRESTPayloadAndAssertPoint(t *testing.T, payload *tools.Payload, start, end int64) {

	payload.Tags["ksid"] = ksMycenae
	payload.Tags["ttl"] = "1"

	ps := tools.PayloadSlice{PS: []tools.Payload{*payload}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetHashFromMetricAndTags(payload.Metric, payload.Tags)

	assertMycenae(t, ksMycenae, start, end, *payload.Value, hashID)

	assertElastic(t, ksMycenae, payload.Metric, payload.Tags, hashID)
}

// payload must be composed by point(s) with Timestamp(s) != nil
func sendRESTPayloadWithMoreThanAPointAndAssertPoints(t *testing.T, payload tools.PayloadSlice) {

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", payload.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)

	time.Sleep(tools.Sleep3)

	for _, point := range payload.PS {

		hashID := tools.GetTSUIDFromPayload(&point, true)

		assertMycenae(t, ksMycenae, *point.Timestamp, *point.Timestamp, *point.Value, hashID)

		assertElastic(t, ksMycenae, point.Metric, point.Tags, hashID)
	}
}

func sendRESTPayloadStringAndAssertEmpty(t *testing.T, payload, metric string, tags map[string]string, start, end int64) {

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 400, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetHashFromMetricAndTags(metric, tags)

	assertMycenaeEmpty(t, ksMycenae, start, end, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}

// Payload must represent an array of length = 1
func sendRESTPayloadStringAndAssertErrorAndEmpty(t *testing.T, errMessage, payload, keyspace, metric string, tags map[string]string, start, end int64) {

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 400, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetHashFromMetricAndTags(metric, tags)

	assertMycenaeEmpty(t, ksMycenae, start, end, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}

func sendRESTPayloadWithMoreThanAPointAndAssertError(t *testing.T, errMessage string, payload tools.PayloadSlice, invalidPointPosition int) {

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", payload.Marshal())
	assert.Equal(t, 400, statusCode)

	invalidPoint := &payload.PS[invalidPointPosition]

	hashID := tools.GetHashFromMetricAndTags(invalidPoint.Metric, invalidPoint.Tags)

	assertMycenaeEmpty(t, ksMycenae, *invalidPoint.Timestamp, *invalidPoint.Timestamp, hashID)

	assertElasticEmpty(t, ksMycenae, invalidPoint.Metric, invalidPoint.Tags, hashID)
}
