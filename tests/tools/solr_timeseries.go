package tools

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"time"
)

type SolrResult struct {
	Response SolrResponse `json:response`
}

type SolrResponse struct {
	Total int            `json:"numFound"`
	Docs  []SolrDocument `json:"docs"`
}

type SolrDocument struct {
	ID       string   `json:"id"`
	Metric   string   `json:"metric"`
	TSType   string   `json:"type"`
	TagKey   []string `json:"tagKey"`
	TagValue []string `json:"tagValue"`
}

type esTs struct {
	httpT *httpTool
}

func (ts *esTs) init(httpT *httpTool) {
	ts.httpT = httpT
}

func (ts *esTs) extractResponse(path string) SolrResponse {
	_, response, _ := ts.httpT.GET(path)
	r := SolrResult{}
	json.Unmarshal(response, &r)
	return r.Response
}

func (ts *esTs) escapeSpecialChars(value string) string {
	re := regexp.MustCompile("([^a-zA-Z0-9]{1})")
	return re.ReplaceAllString(value, "\\$0")
}

func (ts *esTs) GetMetricPost(ksid, metric string) int {
	q := url.QueryEscape(fmt.Sprintf("metric:%s AND type:meta", ts.escapeSpecialChars(metric)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	r := ts.extractResponse(path)
	return r.Total
}

func (ts *esTs) GetTagValuePost(ksid, tagValue string) int {
	q := url.QueryEscape(fmt.Sprintf("tagValue:%s AND type:meta", ts.escapeSpecialChars(tagValue)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	r := ts.extractResponse(path)
	return r.Total
}

func (ts *esTs) GetTagKeyPost(ksid, tagKey string) int {
	q := url.QueryEscape(fmt.Sprintf("tagKey:%s AND type:meta", ts.escapeSpecialChars(tagKey)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	r := ts.extractResponse(path)
	return r.Total
}

func (ts *esTs) GetTextMetricPost(ksid, metric string) int {
	q := url.QueryEscape(fmt.Sprintf("metric:%s AND type:metatext", ts.escapeSpecialChars(metric)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	return ts.extractResponse(path).Total
}

func (ts *esTs) GetTextTagValuePost(ksid, tagValue string) int {
	q := url.QueryEscape(fmt.Sprintf("tagValue:%s AND type:metatext", ts.escapeSpecialChars(tagValue)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	return ts.extractResponse(path).Total
}

func (ts *esTs) GetTextTagKeyPost(ksid, tagKey string) int {
	q := url.QueryEscape(fmt.Sprintf("tagKey:%s AND type:metatext", ts.escapeSpecialChars(tagKey)))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=0&wt=json", ksid, q)
	return ts.extractResponse(path).Total
}

func (ts *esTs) GetMeta(ksid, hash string) *SolrDocument {
	q := url.QueryEscape(fmt.Sprintf("id:%s AND type:meta", hash))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=1&wt=json", ksid, q)
	r := ts.extractResponse(path)
	if r.Total == 0 {
		return nil
	}
	return &r.Docs[0]
}

func (ts *esTs) GetTextMeta(ksid, hash string) *SolrDocument {
	q := url.QueryEscape(fmt.Sprintf("id:%s AND type:metatext", hash))
	path := fmt.Sprintf("solr/%s/select?q=%s&rows=1&wt=json", ksid, q)
	r := ts.extractResponse(path)
	if r.Total == 0 {
		return nil
	}
	return &r.Docs[0]
}

type UpdateJSON struct {
	Delete DeleteJSON `json:"delete"`
}

type DeleteJSON struct {
	Query string `json:"query"`
}

func (ts *esTs) DeleteKey(ksid, tsid string) error {
	updateJSON := UpdateJSON{
		Delete: DeleteJSON{
			Query: fmt.Sprintf("id:%s", tsid),
		},
	}
	path := fmt.Sprintf("solr/%s/update?commit=true", ksid)
	payload, _ := json.Marshal(updateJSON)
	headers := map[string]string{"content-type": "application/json"}
	code, content, err := ts.httpT.CustomHeaderPOST(path, payload, headers)
	time.Sleep(Sleep2)
	if code != 200 {
		return fmt.Errorf(
			"It was not possible to delete the key %s from the Elastic Search.\nStatus: %d.\nMessage: %s.\nError: %v",
			tsid,
			code,
			string(content),
			err,
		)
	}
	if len(content) == 0 {
		return fmt.Errorf("The solr search server provided an invalid response: %d", code)
	}
	return err
}
