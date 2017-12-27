package main

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var lookupMetaIDs []string
var lookupMetas map[string]tools.TsMeta

func sendPointsMetadata(keyspace string) {

	fmt.Println("Setting up metadata_test.go tests...")

	lookupMetaIDs = []string{"724801524", "1035465811", "3442582840", "3451804462"}

	metricX := "os.cpuTest"
	metricY := "execution.time"
	metricZ := "os-_/.%&#;cpu"

	tagKx := "hos-_/.%&#;t"
	tagVx := "a1-test-_/.%&#;Meta"
	tagKy := "hostName"
	tagVz := "a2-testMeta"

	lookupMetas = map[string]tools.TsMeta{
		lookupMetaIDs[0]: {Metric: metricX, Tags: map[string]string{tagKx: tagVz}},
		lookupMetaIDs[1]: {Metric: metricY, Tags: map[string]string{tagKy: tagVx}},
		lookupMetaIDs[2]: {Metric: metricZ, Tags: map[string]string{tagKx: tagVx}},
		lookupMetaIDs[3]: {Metric: metricZ, Tags: map[string]string{tagKx: tagVz}},
	}

	point := `[
	  {
		"value": 36.5,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
		  "` + tagKx + `":"` + tagVx + `"
		}
	  },
	  {
		"value": 54.5,
		"metric": "` + metricX + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVz + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 5.4,
		"metric": "` + metricY + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKy + `":"` + tagVx + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 1.1,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVz + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVx + `"
		}
	  }
	]`

	code, data, _ := mycenaeTools.HTTP.POST("api/put", []byte(point))
	if code != 204 {
		log.Fatal("Error sending points, code: ", code,
			", data: ", string(data),
			" metadata_test.go")
	}

	pointT := `[
	  {
		"text": "test1",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVx + `"
		}
	  },
	  {
		"text": "test2",
		"metric": "` + metricX + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVz + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"text": "test3",
		"metric": "` + metricY + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKy + `":"` + tagVx + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"text": "test4",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVz + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"text": "test5",
		"metric": "` + metricZ + `",
		"tags": {
		  "ksid": "` + keyspace + `",
	      "` + tagKx + `":"` + tagVx + `"
		}
	  }
	]`

	code, _, _ = mycenaeTools.HTTP.POST("v2/text", []byte(pointT))
	if code != 204 {
		log.Fatal("Error sending text points, code: ", code, " metadata_test.go")
	}
}

func TestListMetadata(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
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
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

}

func TestListMetadataAllParameters(t *testing.T) {

	payload := `{
	  "metric":"os-_/.%\\&\\#;cpu",
	  "tags":[
	    {
	      "tagKey":"hos-_/.%\\&\\#;t",
	      "tagValue":"a1-test-_/.%\\&\\#;Meta"
	    }
      ]
    }`

	url := fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 1, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

}

func TestListMetadataMetricWithRegex(t *testing.T) {

	payload := `{
	  "metric":"os.*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
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
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 3, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
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

	url := fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
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
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
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

	url := fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
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
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

}

func TestListMetadataNoResult(t *testing.T) {

	payload := `{
	  "metric":"invalidMetric"
    }`

	code, response, err := mycenaeTools.HTTP.POST(fmt.Sprintf("keyspaces/%s/meta", ksMycenaeMeta), []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 204, code)
	assert.Empty(t, response)

	code, response, err = mycenaeTools.HTTP.POST(fmt.Sprintf("keyspaces/%s/text/meta", ksMycenaeMeta), []byte(payload))

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

	url := fmt.Sprintf("keyspaces/%s/meta?size=1", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 1, len(response.Payload))

	url = fmt.Sprintf("keyspaces/%s/text/meta?size=1", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 1, len(response.Payload))

}

func TestListMetadataSizeTwo(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/text/meta?size=2", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

	url = fmt.Sprintf("keyspaces/%s/text/meta?size=2", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

}

func TestListMetadataFromOne(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta?from=1", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 3, len(response.Payload))

	url = fmt.Sprintf("keyspaces/%s/text/meta?from=1", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 3, len(response.Payload))

}

func TestListMetadataFromTwo(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta?from=2", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

	url = fmt.Sprintf("keyspaces/%s/text/meta?from=2", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)
	assert.Equal(t, 2, len(response.Payload))

}

func TestListMetadataOnlyIDSTrue(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta?onlyids=true", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	var emptyTag map[string]string

	for _, payload := range response.Payload {

		switch payload.TsID {
		case lookupMetaIDs[0]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case lookupMetaIDs[1]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case lookupMetaIDs[2]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case lookupMetaIDs[3]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta?onlyids=true", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case "T" + lookupMetaIDs[1]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, "", payload.Metric)
			assert.Equal(t, emptyTag, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

}

func TestListMetadataOnlyIDSFalse(t *testing.T) {

	payload := `{
	  "metric":".*"
    }`

	url := fmt.Sprintf("keyspaces/%s/meta?onlyids=false", ksMycenaeMeta)
	code, response := requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
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
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

	url = fmt.Sprintf("keyspaces/%s/text/meta?onlyids=false", ksMycenaeMeta)
	code, response = requestResponse(t, url, payload)

	assert.Equal(t, 200, code)
	assert.Equal(t, 4, response.TotalRecord)

	for _, payload := range response.Payload {

		switch payload.TsID {
		case "T" + lookupMetaIDs[0]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[0]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[1]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[1]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[2]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[2]].Tags, payload.Tags)
		case "T" + lookupMetaIDs[3]:
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Metric, payload.Metric)
			assert.Equal(t, lookupMetas[lookupMetaIDs[3]].Tags, payload.Tags)
		default:
			t.Error("Unexpected ID, ", payload.TsID)
		}
	}

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
