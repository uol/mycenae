package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

// HELPERS

func postPoints(payload interface{}, text bool, t *testing.T) {

	var x interface{}
	var statusCode int

	if text {
		statusCode = mycenaeTools.HTTP.POSTjson("api/text/put", payload, x)
	} else {
		statusCode = mycenaeTools.HTTP.POSTjson("api/put", payload, x)
	}

	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)
}

func getResponse(path string, total, length int, t *testing.T) tools.ResponseMetricTags {

	fullPath := fmt.Sprintf("keysets/%v/%v", ksMycenae, path)
	resp := tools.ResponseMetricTags{}
	statusCode := mycenaeTools.HTTP.GETjson(fullPath, &resp)

	assert.Equal(t, 200, statusCode)
	assert.Equal(t, total, resp.TotalRecords)
	assert.Equal(t, length, len(resp.Payload))

	return resp
}

// TESTS

// METRIC

func TestListMetricV2(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	payload := []tools.Payload{*p}

	postPoints(payload, false, t)

	path := fmt.Sprintf("metrics?metric=%v", p.Metric)
	resp := getResponse(path, 1, 1, t)

	assert.True(t, resp.Payload[0] == p.Metric)
}

func TestListMetricV2Regex(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

		if (i+1)%2 == 0 {
			p.Metric = fmt.Sprint(p.Metric, "r")
		}
		payload[i] = *p
	}

	postPoints(payload, false, t)

	path := fmt.Sprintf("metrics?metric=%s%s", tools.MetricForm, ".*r{1}")
	resp := getResponse(path, len(payload)/2, len(payload)/2, t)

	for i := range resp.Payload {
		metric := resp.Payload[i]
		assert.Contains(t, metric, tools.MetricForm)
		assert.Equal(t, metric[len(metric)-1:], "r")
	}
}

func TestListMetricV2Empty(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
		payload[i] = *p
	}

	postPoints(payload, false, t)

	statusCode, _, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%v/metrics?metric=x", ksMycenae))
	assert.Equal(t, 204, statusCode)
}

func TestListMetricV2Size(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
		p.Metric = fmt.Sprint(p.Metric, "b")
		payload[i] = *p
	}

	postPoints(payload, false, t)

	path := fmt.Sprintf("metrics?metric=%s%s", tools.MetricForm, ".*b{1}")
	resp := getResponse(path, 5, 5, t)

	path = fmt.Sprintf("metrics?metric=%s%s&size=2", tools.MetricForm, ".*b{1}")
	resp2 := getResponse(path, 5, 2, t)

	assert.Equal(t, resp.Payload[0], resp2.Payload[0])
	assert.Equal(t, resp.Payload[1], resp2.Payload[1])
}

// METRIC TEXT

func TestListMetricV2TextRegex(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

		if (i+1)%2 == 0 {
			p.Metric = fmt.Sprint(p.Metric, "m")
		}
		payload[i] = *p
	}

	postPoints(payload, true, t)

	path := fmt.Sprintf("text/metrics?metric=%s%s", tools.MetricForm, ".*m{1}")
	resp := getResponse(path, len(payload)/2, len(payload)/2, t)

	for i := range resp.Payload {
		metric := resp.Payload[i]
		assert.Contains(t, metric, tools.MetricForm)
		assert.Equal(t, metric[len(metric)-1:], "m")
	}
}

func TestListMetricV2TextEmpty(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
		payload[i] = *p
	}

	postPoints(payload, true, t)

	statusCode, _, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%v/text/metrics?metric=x", ksMycenae))
	assert.Equal(t, 204, statusCode)
}

func TestListMetricV2TextSize(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
		p.Metric = fmt.Sprint(p.Metric, "s")
		payload[i] = *p
	}

	postPoints(payload, true, t)

	path := fmt.Sprintf("text/metrics?metric=%s%s", tools.MetricForm, ".*s{1}")
	resp := getResponse(path, 5, 5, t)

	path = fmt.Sprintf("text/metrics?metric=%s%s&size=2", tools.MetricForm, ".*s{1}")
	resp2 := getResponse(path, 5, 2, t)

	assert.Equal(t, resp.Payload[0], resp2.Payload[0])
	assert.Equal(t, resp.Payload[1], resp2.Payload[1])
}

// TAGS

func TestListTagsV2(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	payload := []tools.Payload{*p}

	postPoints(payload, false, t)

	path := fmt.Sprintf("tags?tag=%v", p.TagKey)
	resp := getResponse(path, 1, 1, t)

	_, found := payload[0].Tags[resp.Payload[0]]
	assert.Equal(t, true, found)
}

func TestListTagsV2Regex(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

		if (i+1)%2 == 0 {
			tagKey := fmt.Sprint(p.TagKey, "x")
			p.Tags[tagKey] = p.TagValue
		}
		payload[i] = *p
	}

	postPoints(payload, false, t)

	path := fmt.Sprintf("tags?tag=%s%s", tools.TagKeyForm, ".*x{1}")
	resp := getResponse(path, len(payload)/2, len(payload)/2, t)

	for i := range resp.Payload {
		tagKey := resp.Payload[i]
		assert.Contains(t, tagKey, tools.TagKeyForm)
		assert.Equal(t, tagKey[len(tagKey)-1:], "x")
	}
}

func TestListTagV2Empty(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {

		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
		payload[i] = *p
	}

	postPoints(payload, false, t)

	statusCode, _, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%v/tags?tag=x", ksMycenae))
	assert.Equal(t, 204, statusCode)
}

func TestListTagsV2Size(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
		tagkey := fmt.Sprint(p.TagKey, "z")
		p.Tags[tagkey] = p.TagValue
		payload[i] = *p
	}

	postPoints(payload, false, t)

	path := fmt.Sprintf("tags?tag=%s%s", tools.TagKeyForm, ".*z{1}")
	resp := getResponse(path, 5, 5, t)

	path = fmt.Sprintf("tags?tag=%s%s&size=2", tools.TagKeyForm, ".*z{1}")
	resp2 := getResponse(path, 5, 2, t)

	assert.Equal(t, resp.Payload[0], resp2.Payload[0])
	assert.Equal(t, resp.Payload[1], resp2.Payload[1])
}

// TAGS TEXT

func TestListTagsV2TextRegex(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)

		if (i+1)%2 == 0 {
			tagKey := fmt.Sprint(p.TagKey, "t")
			p.Tags[tagKey] = p.TagValue
		}
		payload[i] = *p
	}

	postPoints(payload, true, t)

	path := fmt.Sprintf("text/tags?tag=%s%s", tools.TagKeyForm, ".*t{1}")
	resp := getResponse(path, len(payload)/2, len(payload)/2, t)

	for i := range resp.Payload {
		tagKey := resp.Payload[i]
		assert.Contains(t, tagKey, tools.TagKeyForm)
		assert.Equal(t, tagKey[len(tagKey)-1:], "t")
	}
}

func TestListTagV2TextEmpty(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {

		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
		payload[i] = *p
	}

	postPoints(payload, true, t)

	statusCode, _, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%v/text/tags?tag=x", ksMycenae))
	assert.Equal(t, 204, statusCode)
}

func TestListTagsV2TextSize(t *testing.T) {
	t.Parallel()

	payload := [5]tools.Payload{}

	for i := range payload {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
		tagkey := fmt.Sprint(p.TagKey, "s")
		p.Tags[tagkey] = p.TagValue
		payload[i] = *p
	}

	postPoints(payload, true, t)

	path := fmt.Sprintf("text/tags?tag=%s%s", tools.TagKeyForm, ".*s{1}")
	resp := getResponse(path, 5, 5, t)

	path = fmt.Sprintf("text/tags?tag=%s%s&size=2", tools.TagKeyForm, ".*s{1}")
	resp2 := getResponse(path, 5, 2, t)

	assert.Equal(t, resp.Payload[0], resp2.Payload[0])
	assert.Equal(t, resp.Payload[1], resp2.Payload[1])
}

func populateTagByMetric(metric string, size int, t *testing.T) *tools.Payload {

	var value float32 = 1.1

	p := &tools.Payload{
		Value:  &value,
		Metric: metric,
		Tags: map[string]string{
			"ttl":  "1",
			"ksid": ksMycenae,
		},
	}

	for i := 0; i < size; i++ {
		p.Tags[metric+"-tag-"+strconv.Itoa(i)] = metric + "-key-" + strconv.Itoa(i)
	}

	payload := []tools.Payload{*p}
	postPoints(payload, false, t)

	return p
}

func TestTagKeysByMetricAllTags(t *testing.T) {
	t.Parallel()

	size := 3
	expectedSize := 4 //plus TTL
	p := populateTagByMetric("TestTagKeysByMetricAllTags", size, t)

	path := fmt.Sprintf("metric/tag/keys?metric=%s", p.Metric)
	resp := getResponse(path, expectedSize, expectedSize, t)

	tagMap := map[string]bool{}
	for i := 0; i < expectedSize; i++ {
		tagMap[resp.Payload[i]] = true
	}

	for _, v := range p.Tags {
		assert.True(t, tagMap[v])
	}
}

func TestTagKeysByMetricRegex(t *testing.T) {
	t.Parallel()

	size := 10
	expectedSize := 5
	p := populateTagByMetric("TestTagKeysByMetricRegex", size, t)

	path := fmt.Sprintf("metric/tag/keys?metric=%s&tag=%s", p.Metric, url.QueryEscape("TestTagKeysByMetricRegex\\-tag\\-[02468]+"))
	resp := getResponse(path, expectedSize, expectedSize, t)

	tagMap := map[string]bool{
		"tag0": true,
		"tag2": true,
		"tag4": true,
		"tag6": true,
		"tag8": true,
	}

	for i := 0; i < expectedSize; i++ {
		assert.True(t, tagMap[resp.Payload[i]])
	}
}

func TestTagKeysByMetricMaxResults(t *testing.T) {
	t.Parallel()

	size := 10
	expectedSize := 11
	expectedCropped := 6
	p := populateTagByMetric("TestTagKeysByMetricMaxResults", size, t)

	path := fmt.Sprintf("metric/tag/keys?metric=%s&size=%d", p.Metric, expectedSize)
	getResponse(path, expectedSize, expectedCropped, t)
}

func populateTagsWithDifferentValues(t *testing.T, metric string, tagValue ...string) []tools.Payload {

	payload := []tools.Payload{}

	var value float32 = 1.2

	for i := 0; i < len(tagValue); i++ {

		p := &tools.Payload{
			Value:  &value,
			Metric: metric,
			Tags: map[string]string{
				"ttl":  "1",
				"ksid": ksMycenae,
				"tag":  tagValue[i],
			},
		}

		payload = append(payload, *p)
	}

	postPoints(payload, false, t)

	return payload
}

func TestTagValuesByMetricAllTags(t *testing.T) {
	t.Parallel()

	expectedSize := 3
	payload := populateTagsWithDifferentValues(t, "TestTagValuesByMetricAllTags", "tag1", "tag2", "tag3")

	path := fmt.Sprintf("metric/tag/values?metric=%s&tag=%s", "TestTagValuesByMetricAllTags", "tag")
	resp := getResponse(path, expectedSize, expectedSize, t)

	tagMap := map[string]bool{}
	for i := 0; i < expectedSize; i++ {
		tagMap[resp.Payload[i]] = true
	}

	for i := 0; i < expectedSize; i++ {
		assert.True(t, tagMap[payload[i].Tags["tag"]])
	}
}

func TestTagValuesByMetricRegex(t *testing.T) {
	t.Parallel()

	size := 2
	populateTagsWithDifferentValues(t, "TestTagValuesByMetricRegex", "tag1", "lalala", "tag3")

	path := fmt.Sprintf("metric/tag/values?metric=%s&tag=tag&value=%s", "TestTagValuesByMetricRegex", url.QueryEscape("tag[0-9]+"))
	resp := getResponse(path, size, size, t)

	tagMap := map[string]bool{
		"tag1": true,
		"tag3": true,
	}

	for i := 0; i < size; i++ {
		assert.True(t, tagMap[resp.Payload[i]])
	}
}

func TestTagValuesByMetricMaxResults(t *testing.T) {
	t.Parallel()

	size := 3
	expectedSize := 1
	populateTagsWithDifferentValues(t, "TestTagValuesByMetricMaxResults", "tag1", "tag2", "tag3")

	path := fmt.Sprintf("metric/tag/values?metric=%s&tag=tag&size=%d", "TestTagValuesByMetricMaxResults", expectedSize)
	getResponse(path, size, expectedSize, t)
}
