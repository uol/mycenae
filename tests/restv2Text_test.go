package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func assertElasticText(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	lenTags := len(tags) - 1 // no ksid tag

	count := mycenaeTools.Solr.Timeseries.GetTextMetricPost(keyspace, metric)
	assert.Equal(t, 1, count, "metric sent in the payload does not match the one in solr")

	meta := mycenaeTools.Solr.Timeseries.GetTextMeta(keyspace, hashID)
	assert.Equal(t, hashID, meta.ID, "meta id corresponding to the payload does not match the one in solr")
	assert.Equal(t, metric, meta.Metric, "metric sent in the payload does not match the one in solr")
	assert.Equal(t, lenTags, len(meta.Tags))

	for i := 0; i < len(meta.Tags); i++ {

		if meta.Tags[i].TagKey == "ttl" {
			continue
		}

		_, ok := tags[meta.Tags[i].TagKey]
		assert.True(t, ok, "tag key in solr was not sent in payload")

		count := mycenaeTools.Solr.Timeseries.GetTextTagKeyPost(keyspace, meta.Tags[i].TagKey)
		assert.Equal(t, 1, count, "tag key sent in the payload does not match the one in solr")

		count = mycenaeTools.Solr.Timeseries.GetTextTagValuePost(keyspace, meta.Tags[i].TagValue)
		assert.Equal(t, 1, count, "tag value sent in the payload does not match the one in solr")
	}
}

func assertElasticTextEmpty(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	count := mycenaeTools.Solr.Timeseries.GetTextMetricPost(keyspace, metric)
	assert.Equal(t, 0, count, fmt.Sprintf("text metric sent in the payload does not match the one in solr: m=%s, tags=%+v, ks=%s", metric, tags, keyspace))

	for tagKey, tagValue := range tags {

		if tagKey == "ttl" {
			continue
		}

		count = mycenaeTools.Solr.Timeseries.GetTextTagKeyPost(keyspace, tagKey)
		assert.Equal(t, 0, count)

		count = mycenaeTools.Solr.Timeseries.GetTextTagValuePost(keyspace, tagValue)
		assert.Equal(t, 0, count)
	}

	meta := mycenaeTools.Solr.Timeseries.GetTextMeta(keyspace, hashID)
	assert.Nil(t, meta, "document has been found when it should not")
}

func assertMycenaeText(t *testing.T, keyspace string, start int64, end int64, value string, hashID string) {

	status, response := mycenaeTools.Mycenae.GetTextPoints(keyspace, start, end, hashID)

	if assert.Equal(t, 200, status) {
		if assert.Equal(t, 1, len(response.Payload[hashID].Points.Ts)) {
			assert.True(t, start*1000 <= int64(response.Payload[hashID].Points.Ts[0][0].(float64)) && int64(response.Payload[hashID].Points.Ts[0][0].(float64)) <= end*1000)
			assert.Equal(t, value, response.Payload[hashID].Points.Ts[0][1].(string))
		}
	}
}

func assertMycenaeTextEmpty(t *testing.T, keyspace string, start int64, end int64, hashID string) {

	status, response := mycenaeTools.Mycenae.GetTextPoints(keyspace, start, end, hashID)

	assert.Equal(t, 204, status)
	assert.Equal(t, tools.MycenaePointsText{}, response)
}

func assertCassandraText(t *testing.T, hashID string, text string, start, end int64) {

	cassValue := mycenaeTools.Cassandra.Timeseries.GetTextFromTwoDatesSTAMP(1, hashID, time.Unix(start, 0), time.Unix(end, 0))
	assert.Equal(t, text, cassValue)
}

func assertCassandraTextEmpty(t *testing.T, hashID string, start, end int64) {

	cassValue := mycenaeTools.Cassandra.Timeseries.GetTextFromTwoDatesSTAMP(1, hashID, time.Unix(start, 0), time.Unix(end, 0))
	assert.Equal(t, "", cassValue)
}

// Asserts 1 error
/*func assertRESTextError(t *testing.T, err tools.RestErrors, payload *tools.Payload, keyspace, errMessage string, lenFailed, lenSuccess int) {

	if assert.Equal(t, 1, len(err.Errors)) {

		if payload.Metric == "" {
			assert.Empty(t, err.Errors[0].Datapoint.Metric)
		} else {
			assert.Equal(t, payload.Metric, err.Errors[0].Datapoint.Metric)
		}

		if payload.Text != nil && *payload.Text != "" {

			assert.Equal(t, *payload.Text, err.Errors[0].Datapoint.Text)
		}

		assert.Equal(t, len(payload.Tags), len(err.Errors[0].Datapoint.Tags))

		for tagK, tagV := range err.Errors[0].Datapoint.Tags {

			payTagV, ok := payload.Tags[tagK]
			if assert.True(t, ok, "tag key was not sent in payload") {
				assert.Equal(t, tagV, payTagV)
			}
		}

		assert.Equal(t, keyspace, err.Errors[0].Datapoint.Tags["ksid"])
		assert.Contains(t, err.Errors[0].Error, errMessage)
	}
	assert.Equal(t, lenFailed, err.Failed)
	assert.Equal(t, lenSuccess, err.Success)
}*/

func TestRESTv2TextPayloadWithAllFields(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Timestamp = nil

	dateBefore := time.Now().Unix()

	sendRESTextPayloadAndAssertPoint(t, p, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2TextPayloadWithMoreThanOneTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p.TagKey2 = fmt.Sprint("testTagKey2-", p.Random)
	p.TagValue2 = fmt.Sprint("testTagValue2-", p.Random)
	p.Tags[p.TagKey2] = p.TagValue2

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextMultiplePointsSameIDAndTimestampsGreaterThanDay(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	timestamps := [5]int64{
		*p.Timestamp,
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -1).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -2).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -3).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -4).Unix(),
	}

	values := [5]string{
		"text 1",
		"text 2",
		"text 3",
		"text 4",
		"text 5",
	}

	for i := 0; i < 5; i++ {

		*p.Timestamp = timestamps[i]
		*p.Text = values[i]

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
		assert.Equal(t, http.StatusNoContent, statusCode)
	}

	hashID := tools.GetTextHashFromMetricAndTags(p.Metric, p.Tags)

	time.Sleep(tools.Sleep3)

	for i := 0; i < 5; i++ {

		assertMycenaeText(t, ksMycenae, timestamps[i], timestamps[i], values[i], hashID)
		assertCassandraText(t, hashID, values[i], timestamps[i], timestamps[i])
	}

	assertElasticText(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2TextMultiplePointsSameIDAndNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Timestamp = nil
	dateBefore := time.Now()

	hashID := tools.GetTextHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 3; i++ {

		*p.Text = string(i)

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
		assert.Equal(t, http.StatusNoContent, statusCode)

		time.Sleep(tools.Sleep3)

		dateAfter := time.Now()
		assertMycenaeText(t, ksMycenae, dateBefore.Unix(), dateAfter.Unix(), *p.Text, hashID)
		assertCassandraText(t, hashID, *p.Text, dateBefore.Unix(), dateAfter.Unix())
		dateBefore = dateAfter
	}

	assertElasticText(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2TextPayloadWithOnlyNumbersOrLetters(t *testing.T) {
	t.Parallel()

	numbers := "01234567890123456789"
	letters := "abcdefghijklmnopqrstuvwxyzabcd"

	numbersOrLetters := []string{numbers, letters}

	for _, numOrLetters := range numbersOrLetters {

		var wg sync.WaitGroup
		wg.Add(4)

		go testTextValue(t, numOrLetters, &wg)
		go testTextMetric(t, numOrLetters, &wg)
		go testTextTagKey(t, numOrLetters, &wg)
		go testTextTagValue(t, numOrLetters, &wg)

		wg.Wait()
	}
}

func TestRESTv2TextPayloadWithSpecialChars(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(tests))

	for test := range tests {

		go func(test int) {

			p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

			specialChars := fmt.Sprint("9Aa35ffg", p.Random, "...-___-.%&#;./.a1")

			switch test {
			case 0:
				p.Metric = specialChars
				*p.Text = string(test)
			case 1:
				p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", specialChars: p.TagValue}
				*p.Text = string(test)
			case 2:
				p.Tags[p.TagKey] = specialChars
				*p.Text = string(test)
			}

			ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

			statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
			assert.Equal(t, http.StatusNoContent, statusCode)

			// special chars take longer to be saved
			time.Sleep(tools.Sleep3 * 2)

			hashID := tools.GetTextHashFromMetricAndTags(p.Metric, p.Tags)

			assertMycenaeText(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Text, hashID)

			assertCassandraText(t, hashID, *p.Text, *p.Timestamp, *p.Timestamp)

			assertElasticText(t, ksMycenae, p.Metric, p.Tags, hashID)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2TextPayloadWithLastCharUnderscore(t *testing.T) {
	t.Parallel()

	lastCharUnderscore := fmt.Sprint("9Aa35ffg...-___-..._")

	var wg sync.WaitGroup
	wg.Add(4)

	go testTextValue(t, lastCharUnderscore, &wg)
	go testTextMetric(t, lastCharUnderscore, &wg)
	go testTextTagKey(t, lastCharUnderscore, &wg)
	go testTextTagValue(t, lastCharUnderscore, &wg)

	wg.Wait()
}

func TestRESTv2TextPayloadWithTextAtMax(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	*p.Text = strings.Repeat("a", 10000)

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithTextBiggerThanMax(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	*p.Text = strings.Repeat("b", 10001)

	errMessage := "Wrong Format: Field \"text\" can not have more than 10k"
	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithSameMetricTagsTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)

	hashID := tools.GetTextHashFromMetricAndTags(p.Metric, p.Tags)

	assertMycenaeText(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Text, hashID)

	assertCassandraText(t, hashID, *p.Text, *p.Timestamp, *p.Timestamp)

	assertElasticText(t, ksMycenae, p.Metric, p.Tags, hashID)

	*p.Text = "modified text"

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadsWithSameMetricTagsTimestampTwoEqualTags(t *testing.T) {
	t.Parallel()

	value1 := "text 1"
	value2 := "text 2"
	metric, tagKey1, tagValue1, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()
	tagKey2 := tagKey1
	tagValue2 := tagValue1

	payload1 := fmt.Sprintf(
		`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
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
		`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
		value2,
		metric,
		ksMycenae,
		tagKey1,
		tagValue1,
		tagKey2,
		tagValue2,
		timestamp,
	)

	tags := map[string]string{tagKey1: tagValue1, "ksid": ksMycenae, "ttl": "1"}

	hashID := tools.GetTextHashFromMetricAndTags(metric, tags)

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/text/put", payload1)
	assert.Equal(t, http.StatusNoContent, statusCode)

	statusCode, _ = mycenaeTools.HTTP.POSTstring("api/text/put", payload2)
	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)

	assertMycenaeText(t, ksMycenae, timestamp, timestamp, value2, hashID)

	assertCassandraText(t, hashID, value2, timestamp, timestamp)

	assertElasticText(t, ksMycenae, metric, tags, hashID)
}

func TestRESTv2TextPayloadWithTwoTagsSameKeyAndDifferentValues(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	tagKey2 := p.TagKey
	tagValue2 := fmt.Sprint("testTagValue2-", p.Random)

	payload := fmt.Sprintf(
		`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
		*p.Text,
		p.Metric,
		ksMycenae,
		p.TagKey,
		p.TagValue,
		tagKey2,
		tagValue2,
		*p.Timestamp,
	)

	tags := map[string]string{p.TagKey: tagValue2, "ksid": ksMycenae, "ttl": "1"}

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/text/put", payload)
	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)

	hashID := tools.GetTextHashFromMetricAndTags(p.Metric, tags)

	assertMycenaeText(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Text, hashID)

	assertCassandraText(t, hashID, *p.Text, *p.Timestamp, *p.Timestamp)

	assertElasticText(t, ksMycenae, p.Metric, tags, hashID)

	count := mycenaeTools.Solr.Timeseries.GetTextTagValuePost(ksMycenae, p.TagValue)
	assert.Equal(t, 0, count)
}

func TestRESTv2TextPayloadWithReservedTTLTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p.TagKey2 = "ttl"
	p.TagValue2 = fmt.Sprint("testTagValue2-", p.Random)
	p.Tags[p.TagKey2] = p.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
	assert.Equal(t, http.StatusBadRequest, statusCode)
	time.Sleep(tools.Sleep3)
}

func TestRESTv2TextPayloadWithValueNotSent(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p.Text = nil

	errMessage := "Wrong Format: Field \"text\" is required."

	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithEmptyValues(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// empty value
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text":, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty metric
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric":, "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
			"text 1",
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := ""
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty ksid
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid":, "%v": "%v", "ttl": "1"}, "timestamp": %v}]`,
			"text 2",
			metric,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": "", "ttl": "1", tagKey: tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag key
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", : "%v", "ttl": "1"}, "timestamp": %v}]`,
			"text 3",
			metric,
			ksMycenae,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", "": tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag value
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v":}, "timestamp": %v}]`,
			"text 4",
			metric,
			ksMycenae,
			tagKey,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: ""}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty timestamp
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp":}]`,
			"text 5",
			metric,
			ksMycenae,
			tagKey,
			tagValue,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep3).Unix())

		wg.Done()
	}()
	wg.Wait()
}

func TestRESTv2TextPayloadWithInvalidChars(t *testing.T) {
	t.Parallel()

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	var wgOut sync.WaitGroup
	wgOut.Add(len(invalidChars))

	for _, invalidChar := range invalidChars {

		go func(char string) {

			var wgIn sync.WaitGroup
			wgIn.Add(3)

			go testTextInvalidMetric(t, char, &wgIn)
			go testTextInvalidTagKey(t, char, &wgIn)
			go testTextInvalidTagValue(t, char, &wgIn)

			wgIn.Wait()
			wgOut.Done()

		}(invalidChar)
	}
	wgOut.Wait()
}

func TestRESTv2TextPayloadWithInvalidCharsAtOnce(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	payload := []tools.Payload{}

	timestamp := time.Now().Unix()

	for _, invalidChar := range invalidChars {

		for test := range tests {

			p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
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

	statusCode, _, err := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
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

		assert.Equal(t, *payload[0].Text, restError.Errors[i].Datapoint.Text)
		assert.Equal(t, len(payload[0].Tags), len(restError.Errors[i].Datapoint.Tags))
		assert.Equal(t, payload[0].Tags["ksid"], restError.Errors[i].Datapoint.Tags["ksid"])
		assert.Contains(t, err.Error, "Wrong Format: ")
	}*/

	for _, p := range payload {

		hashID := tools.GetTextHashFromMetricAndTags(p.Metric, p.Tags)

		assertMycenaeTextEmpty(t, p.Tags["ksid"], timestamp, timestamp, hashID)
		assertCassandraTextEmpty(t, hashID, timestamp, timestamp)
		assertElasticTextEmpty(t, p.Tags["ksid"], p.Metric, p.Tags, hashID)
	}
}

func TestRESTv2TextPayloadValuesWithOnlySpace(t *testing.T) {
	t.Parallel()

	space := " "

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// value case
		payload := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
		*payload.Text = space
		sendRESTextPayloadAndAssertPoint(t, payload, *payload.Timestamp, *payload.Timestamp)
		wg.Done()
	}()

	go func() {
		// metric case
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
			"text 1",
			space,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := space
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		errMessage := "Wrong Format: Field \"metric\" has a invalid format."

		sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// keyspace case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
			"text 2",
			metric,
			space,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": space, "ttl": "1", tagKey: tagValue}

		errMessage := "Wrong Format: Tag value has a invalid format."

		sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, space, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag key case
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
			"text 3",
			metric,
			ksMycenae,
			space,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", space: tagValue}

		errMessage := "Wrong Format: Tag key has a invalid format."

		sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag value case
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
			"text 4",
			metric,
			ksMycenae,
			tagKey,
			space,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: space}

		errMessage := "Wrong Format: Tag value has a invalid format."

		sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// timestamp case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
			"text 5",
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			space,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep3).Unix())
		wg.Done()
	}()
	wg.Wait()
}

func TestRESTv2TextPayloadWithAKsidTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	delete(p.Tags, p.TagKey)

	errMessage := `Wrong Format: At least one tag other than "ksid" and "ttl" is required.`
	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithoutKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	delete(p.Tags, "ksid")

	errMessage := "Wrong Format: Tag \"ksid\" is required."

	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), "", p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithInvalidKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Tags["ksid"] = "ksMycenae"

	errMessage := `Wrong Format: Field "ksid" has a invalid format.`

	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2TextPayloadWithInvalidTimestamp(t *testing.T) {
	t.Parallel()

	dateBefore := time.Now().Unix()

	value := "text text"
	metric, tagKey, tagValue, _ := mycenaeTools.Mycenae.GetRandomMetricTags()
	timestamp := 9999999.9

	payload := fmt.Sprintf(
		`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2TextPayloadWithStringTimestamp(t *testing.T) {
	t.Parallel()

	value := "text test"
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": "%v"}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, timestamp, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2TextPayloadWithBadFormatedJson(t *testing.T) {
	t.Parallel()

	value := "text test"
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"text": "%v", "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v": "%v"}, "timestamp": %v`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTextPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue, "ttl": "1", "ksid": ksMycenae}, timestamp, timestamp)
}

func TestRESTv2TextPayloadWithTwoPoints(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p2 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTextPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2TextPayloadWithTwoPointsWithHeader(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTextPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2TextPayloadWithTwoPointsWithTwoTagsEach(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTextPayloadWithMoreThanAPointAndAssertPoints(t, ps)
}

func TestRESTv2TextPayloadWithTwoPointsAndAWrongFormatEmptyString(t *testing.T) {
	t.Parallel()

	cases := make([]byte, 4)

	wg := sync.WaitGroup{}
	wg.Add(len(cases))

	for test := range cases {

		go func(test int) {

			pInvalid := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
			var errMessage string

			switch test {
			case 0:
				*pInvalid.Text = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Field \"text\" is required."
			case 1:
				pInvalid.Metric = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Field \"metric\" has a invalid format."
			case 2:
				pInvalid.TagKey2 = ""
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag key has a invalid format."
			case 3:
				pInvalid.TagValue2 = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag value has a invalid format."
			}

			p2 := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
			p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
			p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
			p2.Tags[p2.TagKey2] = p2.TagValue2

			ps := tools.PayloadSlice{PS: []tools.Payload{*pInvalid, *p2}}

			sendRESTextPayloadWithMoreThanAPointAndAssertError(t, errMessage, ps, 0)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2TextEmptyPayload(t *testing.T) {
	t.Parallel()

	payload := fmt.Sprintf(`[]`)

	statusCode, resp := mycenaeTools.HTTP.POSTstring("api/text/put", payload)
	assert.Equal(t, 400, statusCode)

	expectedErr := tools.Error{
		Error:   "no points",
		Message: "no points",
	}

	receivedErr := tools.Error{}

	err := json.Unmarshal(resp, &receivedErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, expectedErr, receivedErr)
}

func testTextValue(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	*p.Text = value

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextMetric(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Metric = value

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextTagKey(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, "ttl": "1", value: p.TagValue}

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextTagValue(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	sendRESTextPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextInvalidMetric(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Metric = value

	errMessage := "Wrong Format: Field \"metric\" has a invalid format."
	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextInvalidTagKey(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, value: p.TagValue}

	errMessage := "Wrong Format: Tag key has a invalid format."
	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func testTextInvalidTagValue(t *testing.T, value string, wg *sync.WaitGroup) {

	p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	errMessage := "Wrong Format: Tag value has a invalid format."
	sendRESTextPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)

	wg.Done()
}

func sendRESTextPayloadAndAssertPoint(t *testing.T, payload *tools.Payload, start, end int64) {

	ps := tools.PayloadSlice{PS: []tools.Payload{*payload}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", ps.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetTextHashFromMetricAndTags(payload.Metric, payload.Tags)

	assertMycenaeText(t, ksMycenae, start, end, *payload.Text, hashID)

	assertCassandraText(t, hashID, *payload.Text, start, end)

	assertElasticText(t, ksMycenae, payload.Metric, payload.Tags, hashID)
}

// payload must be composed by point(s) with Timestamp(s) != nil
func sendRESTextPayloadWithMoreThanAPointAndAssertPoints(t *testing.T, payload tools.PayloadSlice) {

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", payload.Marshal())
	assert.Equal(t, http.StatusNoContent, statusCode)

	time.Sleep(tools.Sleep3)

	for _, point := range payload.PS {

		hashID := tools.GetTextHashFromMetricAndTags(point.Metric, point.Tags)

		assertMycenaeText(t, ksMycenae, *point.Timestamp, *point.Timestamp, *point.Text, hashID)

		assertCassandraText(t, hashID, *point.Text, *point.Timestamp, *point.Timestamp)

		assertElasticText(t, ksMycenae, point.Metric, point.Tags, hashID)
	}
}

func sendRESTextPayloadStringAndAssertEmpty(t *testing.T, payload, metric string, tags map[string]string, start, end int64) {

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/text/put", payload)
	assert.Equal(t, 400, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetTextHashFromMetricAndTags(metric, tags)

	assertMycenaeTextEmpty(t, ksMycenae, start, end, hashID)

	assertCassandraTextEmpty(t, hashID, start, end)

	assertElasticTextEmpty(t, ksMycenae, metric, tags, hashID)
}

// Payload must represent an array of length = 1
func sendRESTextPayloadStringAndAssertErrorAndEmpty(t *testing.T, errMessage, payload, keyspace, metric string, tags map[string]string, start, end int64) {

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/text/put", payload)
	assert.Equal(t, 400, statusCode)

	/*var restError tools.RestErrors

	err := json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}*/

	payStruct := []tools.Payload{}

	err := json.Unmarshal([]byte(payload), &payStruct)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Len(t, payStruct, 1)

	//assertRESTextError(t, restError, &payStruct[0], keyspace, errMessage, 1, 0)

	time.Sleep(tools.Sleep3)

	hashID := tools.GetTextHashFromMetricAndTags(metric, tags)

	assertMycenaeTextEmpty(t, ksMycenae, start, end, hashID)

	assertCassandraTextEmpty(t, hashID, start, end)

	assertElasticTextEmpty(t, ksMycenae, metric, tags, hashID)
}

func sendRESTextPayloadWithMoreThanAPointAndAssertError(t *testing.T, errMessage string, payload tools.PayloadSlice, invalidPointPosition int) {

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/text/put", payload.Marshal())
	assert.Equal(t, 400, statusCode)

	/*var restError tools.RestErrors

	err := json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}*/

	invalidPoint := &payload.PS[invalidPointPosition]

	//assertRESTextError(t, restError, invalidPoint, ksMycenae, errMessage, 1, len(payload.PS)-1)

	hashID := tools.GetTextHashFromMetricAndTags(invalidPoint.Metric, invalidPoint.Tags)

	assertMycenaeTextEmpty(t, ksMycenae, *invalidPoint.Timestamp, *invalidPoint.Timestamp, hashID)

	assertCassandraTextEmpty(t, hashID, *invalidPoint.Timestamp, *invalidPoint.Timestamp)

	assertElasticTextEmpty(t, ksMycenae, invalidPoint.Metric, invalidPoint.Tags, hashID)

	/*time.Sleep(tools.Sleep3)

	for index, point := range payload.PS {

		if index != invalidPointPosition {

			hashID := tools.GetTextHashFromMetricAndTags(point.Metric, point.Tags)

			assertMycenaeText(t, ksMycenae, *point.Timestamp, *point.Timestamp, *point.Text, hashID)

			assertCassandraText(t, hashID, *point.Text, *point.Timestamp, *point.Timestamp)

			assertElasticText(t, ksMycenae, point.Metric, point.Tags, hashID)
		}
	}*/
}
