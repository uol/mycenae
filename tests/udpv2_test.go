package main

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/uol/mycenae/tests/tools"
)

func TestUDPv2PayloadWithAllFields(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil

	dateBefore := time.Now().Unix()

	sendUDPPayloadAndAssertPoint(t, p, dateBefore, time.Now().Add(tools.Sleep2).Unix())
}

func TestUDPv2PayloadWithMoreThanOneTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p.TagKey2 = fmt.Sprint("testTagKey2-", p.Random)
	p.TagValue2 = fmt.Sprint("testTagValue2-", p.Random)
	p.Tags[p.TagKey2] = p.TagValue2

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2MultiplePointsSameIDAndTimestampsGreaterThanDay(t *testing.T) {
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
		mycenaeTools.UDP.Send(p.Marshal())
	}

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	time.Sleep(tools.Sleep2)

	for i := 0; i < 5; i++ {
		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], values[i], hashID)
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestUDPv2MultiplePointsSameIDAndNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil
	dateBefore := time.Now()

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 3; i++ {

		*p.Value = float32(i)
		mycenaeTools.UDP.Send(p.Marshal())

		time.Sleep(tools.Sleep2)

		dateAfter := time.Now()
		assertMycenae(t, ksMycenae, dateBefore.Unix(), dateAfter.Unix(), *p.Value, hashID)
		dateBefore = dateAfter
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestUDPv2PayloadWithOnlyNumbersOrLetters(t *testing.T) {
	t.Parallel()

	numbers := "01234567890123456789"
	letters := "abcdefghijklmnopqrstuvwxyzabcd"

	numbersOrLetters := []string{numbers, letters}

	for _, numOrLetters := range numbersOrLetters {

		var wg sync.WaitGroup
		wg.Add(3)

		go testMetric(t, numOrLetters, &wg, true)
		go testTagKey(t, numOrLetters, &wg, true)
		go testTagValue(t, numOrLetters, &wg, true)

		wg.Wait()
	}
}

func TestUDPv2PayloadWithSpecialChars(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(tests))

	for test := range tests {

		go func(test int) {

			p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

			specialChars := fmt.Sprint("9Aa35ffg", p.Random, ".-_%&#;/a1")

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

			mycenaeTools.UDP.Send(p.Marshal())

			// special chars take longer to be saved
			time.Sleep(tools.Sleep2 * 2)

			hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

			assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

			assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestUDPv2PayloadWithLastCharUnderscore(t *testing.T) {
	t.Parallel()

	lastCharUnderscore := fmt.Sprint("9Aa35ffg_")

	var wg sync.WaitGroup
	wg.Add(3)

	go testMetric(t, lastCharUnderscore, &wg, true)
	go testTagKey(t, lastCharUnderscore, &wg, true)
	go testTagValue(t, lastCharUnderscore, &wg, true)

	wg.Wait()
}

func TestUDPv2PayloadWithNegativeValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = -5.0

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithZeroValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = 0.0

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithMaxFloat32Value(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = math.MaxFloat32

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadsWithSameMetricTagsTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	mycenaeTools.UDP.Send(p.Marshal())
	time.Sleep(tools.Sleep2)

	hashID := tools.GetHashFromMetricAndTags(p.Metric, p.Tags)

	assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

	*p.Value = 6.1

	sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadsWithSameMetricTagsTimestampTwoEqualTags(t *testing.T) {
	t.Parallel()

	value1 := 5.0
	value2 := 6.0
	metric, tagKey1, tagValue1, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()
	tagKey2 := tagKey1
	tagValue2 := tagValue1

	payload1 := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
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
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
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

	mycenaeTools.UDP.SendString(payload1)
	time.Sleep(tools.Sleep2)
	mycenaeTools.UDP.SendString(payload2)
	time.Sleep(tools.Sleep2)

	assertMycenaeEmpty(t, ksMycenae, timestamp, timestamp, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}

func TestUDPv2PayloadWithStringValue(t *testing.T) {
	t.Parallel()

	value := "testValue"
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"value": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

	sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
}

func TestUDPv2PayloadWithValueNotSent(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Value = nil

	sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithEmptyValues(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// empty value
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value":, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// empty metric
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric":, "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			1.0,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := ""
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// empty ksid
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid":, "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			2.0,
			metric,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": "", "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// empty tag key
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", : "%v", "ttl": "1"}, "timestamp": %v}`,
			3.0,
			metric,
			ksMycenae,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", "": tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// empty tag value
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "ttl": "1", "%v":}, "timestamp": %v}`,
			4.0,
			metric,
			ksMycenae,
			tagKey,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: ""}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// empty timestamp
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp":}`,
			5.0,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep2).Unix())
		wg.Done()
	}()
	wg.Wait()
}

func TestUDPv2PayloadWithInvalidChars(t *testing.T) {
	t.Parallel()

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	var wgOut sync.WaitGroup
	wgOut.Add(len(invalidChars))

	for _, invalidChar := range invalidChars {

		go func(char string) {

			var wgIn sync.WaitGroup
			wgIn.Add(3)

			go testInvalidMetric(t, char, &wgIn, true)
			go testInvalidTagKey(t, char, &wgIn, true)
			go testInvalidTagValue(t, char, &wgIn, true)

			wgIn.Wait()
			wgOut.Done()

		}(invalidChar)
	}
	wgOut.Wait()
}

func TestUDPv2PayloadValuesWithOnlySpace(t *testing.T) {
	t.Parallel()

	space := " "

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// value case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %v, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			space,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// metric case
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			1.0,
			space,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := space
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// keyspace case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			2.0,
			metric,
			space,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": space, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag key case
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			3.0,
			metric,
			ksMycenae,
			space,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", space: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag value case
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			4.0,
			metric,
			ksMycenae,
			tagKey,
			space,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: space}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// timestamp case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
			5.0,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			space,
		)
		tags := map[string]string{"ksid": ksMycenae, "ttl": "1", tagKey: tagValue}

		sendUDPPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep2).Unix())
		wg.Done()
	}()
	wg.Wait()
}

func TestUDPv2PayloadWithAKsidTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, p.TagKey)
	delete(p.Tags, p.TagKey2)
	delete(p.Tags, "ttl")

	sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithoutKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, "ksid")

	sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithInvalidKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags["ksid"] = "ksMycenae"

	sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestUDPv2PayloadWithInvalidTimestamp(t *testing.T) {
	t.Parallel()

	dateBefore := time.Now().Unix()

	value := 5.0
	metric, tagKey, tagValue, _ := mycenaeTools.Mycenae.GetRandomMetricTags()
	timestamp := 9999999.9

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v}`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendUDPPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, dateBefore, time.Now().Add(tools.Sleep2).Unix())
}

func TestUDPv2PayloadWithStringTimestamp(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": "%v"}`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendUDPPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, timestamp, time.Now().Add(tools.Sleep2).Unix())
}

func TestUDPv2PayloadWithBadFormattedJson(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "ttl": "1"}, "timestamp": %v`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendUDPPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, timestamp, timestamp)
}

func sendUDPPayloadAndAssertPoint(t *testing.T, payload *tools.Payload, start, end int64) {

	mycenaeTools.UDP.Send(payload.Marshal())
	time.Sleep(tools.Sleep2)

	hashID := tools.GetHashFromMetricAndTags(payload.Metric, payload.Tags)

	assertMycenae(t, ksMycenae, start, end, *payload.Value, hashID)

	assertElastic(t, ksMycenae, payload.Metric, payload.Tags, hashID)
}

func sendUDPPayloadStringAndAssertEmpty(t *testing.T, payload, metric string, tags map[string]string, start, end int64) {

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(tools.Sleep2)

	hashID := tools.GetHashFromMetricAndTags(metric, tags)

	assertMycenaeEmpty(t, ksMycenae, start, end, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}
