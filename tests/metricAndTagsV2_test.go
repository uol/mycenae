package main

import (
	"fmt"
	"net/http"
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
		statusCode = mycenaeTools.HTTP.POSTjson("v2/text", payload, x)
	} else {
		statusCode = mycenaeTools.HTTP.POSTjson("api/put", payload, x)
	}

	assert.Equal(t, http.StatusNoContent, statusCode)
	time.Sleep(tools.Sleep3)
}

func getResponse(path string, total, length int, t *testing.T) tools.ResponseMetricTags {

	fullPath := fmt.Sprintf("keysets/%v/%v", ksMycenae, path)
	fmt.Println(fullPath)
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

// Does not make sense in the new version
// func TestListMetricV2From(t *testing.T) {
// 	t.Parallel()

// 	payload := [5]tools.Payload{}

// 	for i := range payload {
// 		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
// 		p.Metric = fmt.Sprint(p.Metric, "f")
// 		payload[i] = *p
// 	}

// 	postPoints(payload, false, t)

// 	path := fmt.Sprintf("metrics?metric=%s%s", tools.MetricForm, ".*f{1}")
// 	resp := getResponse(path, 5, 5, t)

// 	path = fmt.Sprintf("metrics?metric=%s%s&from=3", tools.MetricForm, ".*f{1}")
// 	resp2 := getResponse(path, 5, 2, t)

// 	assert.Equal(t, resp.Payload[3], resp2.Payload[0])
// 	assert.Equal(t, resp.Payload[4], resp2.Payload[1])
// }

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

// Does not makes sense in the new version
// func TestListMetricV2TextFrom(t *testing.T) {
// 	t.Parallel()

// 	payload := [5]tools.Payload{}

// 	for i := range payload {
// 		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
// 		p.Metric = fmt.Sprint(p.Metric, "f")
// 		payload[i] = *p
// 	}

// 	postPoints(payload, true, t)

// 	path := fmt.Sprintf("text/metrics?metric=%s%s", tools.MetricForm, ".*f{1}")
// 	resp := getResponse(path, 5, 5, t)

// 	path = fmt.Sprintf("text/metrics?metric=%s%s&from=3", tools.MetricForm, ".*f{1}")
// 	resp2 := getResponse(path, 5, 2, t)

// 	assert.Equal(t, resp.Payload[3], resp2.Payload[0])
// 	assert.Equal(t, resp.Payload[4], resp2.Payload[1])
// }

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

// Does not make sense in the new version
// func TestListTagsV2From(t *testing.T) {
// 	t.Parallel()

// 	payload := [5]tools.Payload{}

// 	for i := range payload {
// 		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
// 		tagkey := fmt.Sprint(p.TagKey, "f")
// 		p.Tags[tagkey] = p.TagValue
// 		payload[i] = *p
// 	}

// 	postPoints(payload, false, t)

// 	path := fmt.Sprintf("tags?tag=%s%s", tools.TagKeyForm, ".*f{1}")
// 	resp := getResponse(path, 5, 5, t)

// 	path = fmt.Sprintf("tags?tag=%s%s&from=3", tools.TagKeyForm, ".*f{1}")
// 	resp2 := getResponse(path, 5, 2, t)

// 	assert.Equal(t, resp.Payload[3], resp2.Payload[0])
// 	assert.Equal(t, resp.Payload[4], resp2.Payload[1])
// }

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

// Does not makes sense in the new version
// func TestListTagsV2TextFrom(t *testing.T) {
// 	t.Parallel()

// 	payload := [5]tools.Payload{}

// 	for i := range payload {
// 		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenae)
// 		tagkey := fmt.Sprint(p.TagKey, "f")
// 		p.Tags[tagkey] = p.TagValue
// 		payload[i] = *p
// 	}

// 	postPoints(payload, true, t)

// 	path := fmt.Sprintf("text/tags?tag=%s%s", tools.TagKeyForm, ".*f{1}")
// 	resp := getResponse(path, 5, 5, t)

// 	path = fmt.Sprintf("text/tags?tag=%s%s&from=3", tools.TagKeyForm, ".*f{1}")
// 	resp2 := getResponse(path, 5, 2, t)

// 	assert.Equal(t, resp.Payload[3], resp2.Payload[0])
// 	assert.Equal(t, resp.Payload[4], resp2.Payload[1])
// }
