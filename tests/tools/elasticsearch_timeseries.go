package tools

import (
	"encoding/json"
	"fmt"
)

type Metric struct {
	ID     string  `json:"_id"`
	Source Msource `json:"_source"`
	Found  bool
}

type Msource struct {
	Metric string `json:"metric"`
}

type TagKey struct {
	ID     string   `json:"_id"`
	Source TKsource `json:"_source"`
	Found  bool
}

type TKsource struct {
	Key string `json:"key"`
}

type TagValue struct {
	ID     string   `json:"_id"`
	Source TVsource `json:"_source"`
	Found  bool
}

type EsPost struct {
	Hits TagValuePostHits `json:"hits"`
}

type TagValuePostHits struct {
	Total int `json:"total"`
}

type TVsource struct {
	Value string `json:"value"`
}

type Meta struct {
	ID     string     `json:"_id"`
	Source Metasource `json:"_source"`
	Found  bool
}

type Metasource struct {
	ID     string    `json:"id"`
	Metric string    `json:"metric"`
	Tags   []TagMeta `json:"tagsNested"`
}

type TagMeta struct {
	TagKey   string `json:"tagKey"`
	TagValue string `json:"tagValue"`
}

type esTs struct {
	httpT *httpTool
}

func (ts *esTs) init(httpT *httpTool) {
	ts.httpT = httpT
}

func (ts *esTs) GetMetricPost(ksid, metric string) (hit EsPost) {
	path := fmt.Sprintf("%v/metric/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"metric":"%v"}}}`, metric)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetTagValuePost(ksid, tagValue string) (hit EsPost) {
	path := fmt.Sprintf("%v/tagv/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"value":"%v"}}}`, tagValue)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetTagKeyPost(ksid, tagKey string) (hit EsPost) {
	path := fmt.Sprintf("%v/tagk/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"key":"%v"}}}`, tagKey)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetTextMetricPost(ksid, metric string) (hit EsPost) {
	path := fmt.Sprintf("%v/metrictext/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"metric":"%v"}}}`, metric)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetTextTagValuePost(ksid, tagValue string) (hit EsPost) {
	path := fmt.Sprintf("%v/tagvtext/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"value":"%v"}}}`, tagValue)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetTextTagKeyPost(ksid, tagKey string) (hit EsPost) {
	path := fmt.Sprintf("%v/tagktext/_search", ksid)
	_, response, _ := ts.httpT.POST(path, []byte(fmt.Sprintf(`{"query":{"term":{"key":"%v"}}}`, tagKey)))
	json.Unmarshal(response, &hit)
	return
}

func (ts *esTs) GetMeta(ksid, hash string) (hit Meta) {
	path := fmt.Sprintf("%v/meta/%v", ksid, hash)
	_ = ts.httpT.GETjson(path, &hit)
	return
}

func (ts *esTs) GetTextMeta(ksid, hash string) (hit Meta) {
	path := fmt.Sprintf("%v/metatext/%v", ksid, hash)
	_ = ts.httpT.GETjson(path, &hit)
	return
}

func (ts *esTs) DeleteKey(ksid, tsid string) error {

	path := fmt.Sprintf("%v/meta/%v", ksid, tsid)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete the key %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tsid,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTextKey(ksid, tsid string) error {

	path := fmt.Sprintf("%v/metatext/%v", ksid, tsid)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete the key %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tsid,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteMetric(ksid, metric string) error {

	path := fmt.Sprintf("%v/metric/%v", ksid, metric)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete metric %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			metric,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTextMetric(ksid, metric string) error {

	path := fmt.Sprintf("%v/metrictext/%v", ksid, metric)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete metric %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			metric,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTagKey(ksid, tagKey string) error {

	path := fmt.Sprintf("%v/tagk/%v", ksid, tagKey)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete tag key %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tagKey,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTextTagKey(ksid, tagKey string) error {

	path := fmt.Sprintf("%v/tagktext/%v", ksid, tagKey)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete tag key %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tagKey,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTagValue(ksid, tagValue string) error {

	path := fmt.Sprintf("%v/tagv/%v", ksid, tagValue)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete tag value %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tagValue,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}

func (ts *esTs) DeleteTextTagValue(ksid, tagValue string) error {

	path := fmt.Sprintf("%v/tagvtext/%v", ksid, tagValue)
	code, content, err := ts.httpT.DELETE(path)
	if code != 200 && code != 201 {
		return fmt.Errorf(
			"It was not possible to delete tag value %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tagValue,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The elastic search server provided an invalid response!")
	}
	return err
}
