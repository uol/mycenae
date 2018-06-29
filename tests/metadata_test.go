package main

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"net/http"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var lookupMetaIDs []string
var lookupMetas map[string]tools.TsMeta

func sendPointsMetadata(keySet string) {

	fmt.Println("Setting up metadata_test.go tests...")

	metricX := "os.cpuTest"
	metricY := "execution.time"
	metricZ := "os.cpu"

	tagKx := "host"
	tagVx := "a1-testMeta"
	tagKy := "hostName"
	tagVz := "a2-testMeta"

	lookupMetaIDs = []string{"m1", "m2", "m3", "m4"}

	lookupMetas = map[string]tools.TsMeta{
		lookupMetaIDs[0]: {Metric: metricX, Tags: map[string]string{"testid": lookupMetaIDs[0], "ttl": "1", tagKx: tagVz}},
		lookupMetaIDs[1]: {Metric: metricY, Tags: map[string]string{"testid": lookupMetaIDs[1], "ttl": "1", tagKy: tagVx}},
		lookupMetaIDs[2]: {Metric: metricZ, Tags: map[string]string{"testid": lookupMetaIDs[2], "ttl": "1", tagKx: tagVx}},
		lookupMetaIDs[3]: {Metric: metricZ, Tags: map[string]string{"testid": lookupMetaIDs[3], "ttl": "1", tagKx: tagVz}},
	}

	point := `[
	  {
		"value": 36.5,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
		  "` + tagKx + `":"` + tagVx + `",
          "testid": "` + lookupMetaIDs[2] + `"
		}
	  },
	  {
		"value": 54.5,
		"metric": "` + metricX + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVz + `",
          "testid": "` + lookupMetaIDs[0] + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 5.4,
		"metric": "` + metricY + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKy + `":"` + tagVx + `",
          "testid": "` + lookupMetaIDs[1] + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 1.1,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVz + `",
          "testid": "` + lookupMetaIDs[3] + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVx + `",
		  "testid": "` + lookupMetaIDs[2] + `"
		}
	  }
	]`

	code, _, _ := mycenaeTools.HTTP.POST("api/put", []byte(point))
	if code != http.StatusNoContent {
		log.Fatal("Error sending points, code: ", code, " metadata_test.go")
	}

	pointT := `[
	  {
		"text": "test1",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVx + `",
		  "testid": "` + lookupMetaIDs[2] + `"
		}
	  },
	  {
		"text": "test2",
		"metric": "` + metricX + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVz + `",
          "testid": "` + lookupMetaIDs[0] + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"text": "test3",
		"metric": "` + metricY + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKy + `":"` + tagVx + `",
          "testid": "` + lookupMetaIDs[1] + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"text": "test4",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVz + `",
          "testid": "` + lookupMetaIDs[3] + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"text": "test5",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagKx + `":"` + tagVx + `",
          "testid": "` + lookupMetaIDs[2] + `"
		}
	  }
	]`

	code, _, _ = mycenaeTools.HTTP.POST("api/text/put", []byte(pointT))
	if code != http.StatusNoContent {
		log.Fatal("Error sending text points, code: ", code, " metadata_test.go")
	}
}

func TestListMetadata(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {
		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestListMetadataAllParameters(t *testing.T) {

	payload := `{
	  "metric":"os.cpu",
	  "tags":[
	    {
	      "tagKey":"host",
	      "tagValue":"a1-testMeta",
          "testid":"m3"
	    }
      ]
    }`

	url := fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestListMetadataMetricWithRegex(t *testing.T) {

	payload := `{
	  "metric":"os.*"
    }`

	url := fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestListMetadataTagKeyWithRegex(t *testing.T) {

	payload := `{
      "tags":[
        {
	      "tagKey":"ho.*"
        }
      ]
    }`

	url := fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestListMetadataTagValueWithRegex(t *testing.T) {

	payload := `{
      "tags":[
        {
	      "tagValue":"a.*"
        }
      ]
    }`

	url := fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestListMetadataNoResult(t *testing.T) {

	payload := `{
	  "metric":"invalidMetric"
    }`

	code, response, err := mycenaeTools.HTTP.POST(fmt.Sprintf("keysets/%s/meta", ksMycenaeMeta), []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 204, code)
	assert.Empty(t, response)

	code, response, err = mycenaeTools.HTTP.POST(fmt.Sprintf("keysets/%s/text/meta", ksMycenaeMeta), []byte(payload))

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 204, code)
	assert.Empty(t, response)

}

func TestListMetadataSizeOne(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta?size=1", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 1, len(response.Payload))

	url = fmt.Sprintf("keysets/%s/text/meta?size=1", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 1, len(response.Payload))

}

func TestListMetadataSizeTwo(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/text/meta?size=2", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

	url = fmt.Sprintf("keysets/%s/text/meta?size=2", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

}

func TestListMetadataFromOne(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta?from=1", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 3, len(response.Payload))

	url = fmt.Sprintf("keysets/%s/text/meta?from=1", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 3, len(response.Payload))

}

func TestListMetadataFromTwo(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta?from=2", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

	url = fmt.Sprintf("keysets/%s/text/meta?from=2", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

}

func TestListMetadataOnlyIDSTrue(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta?onlyids=true", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		assert.True(t, payload.TsID != "")
	}

	url = fmt.Sprintf("keysets/%s/text/meta?onlyids=true", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		assert.True(t, payload.TsID != "")
	}

}

func TestListMetadataOnlyIDSFalse(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keysets/%s/meta?onlyids=false", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

	url = fmt.Sprintf("keysets/%s/text/meta?onlyids=false", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.Tags["testid"] {
		case lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.Tags["testid"])
		}
	}

}

func TestDeleteMetadata(t *testing.T) {

	const lengthPayload = 5
	payloadPoint := [lengthPayload]tools.Payload{}

	payload := fmt.Sprintf(`{
	  "metric":"%s.*"
	}`, tools.MetricForm)

	// ts number //

	for i := range payloadPoint {
		p := mycenaeTools.Mycenae.GetPayload(ksMycenaeMeta)
		payloadPoint[i] = *p
	}

	postPoints(payloadPoint, false, t)

	// commit false
	url := fmt.Sprintf("keysets/%s/delete/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, lengthPayload, response.TotalRecord)
	assert.Equal(t, lengthPayload, len(response.Payload))

	// commit true
	url = fmt.Sprintf("keysets/%s/delete/meta?commit=true", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, lengthPayload, response.TotalRecord)
	assert.Equal(t, lengthPayload, len(response.Payload))

	// commit true again
	code, resp, err := mycenaeTools.HTTP.POST(url, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, http.StatusNoContent, code)
	assert.Empty(t, resp)

	// ts text //

	for i := range payloadPoint {
		p := mycenaeTools.Mycenae.GetTextPayload(ksMycenaeMeta)
		payloadPoint[i] = *p
	}

	postPoints(payloadPoint, true, t)

	// commit false
	url = fmt.Sprintf("keysets/%s/delete/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, lengthPayload, response.TotalRecord)
	assert.Equal(t, lengthPayload, len(response.Payload))

	// commit true
	url = fmt.Sprintf("keysets/%s/delete/text/meta?commit=true", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, lengthPayload, response.TotalRecord)
	assert.Equal(t, lengthPayload, len(response.Payload))

	// commit true again
	code, resp, err = mycenaeTools.HTTP.POST(url, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, http.StatusNoContent, code)
	assert.Empty(t, resp)

}

func requestResponse(t *testing.T, url string, payload string) (int, tools.ResponseMeta) {

	code, resp, err := mycenaeTools.HTTP.POST(url, []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	var response tools.ResponseMeta

	err = json.Unmarshal(resp, &response)
	if err != nil {
		t.Error(err, string(resp))
		t.SkipNow()
	}

	return code, response
}
