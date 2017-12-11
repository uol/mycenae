package tools

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/uol/mycenae/lib/structs"
)

type mycenaeTool struct {
	client *httpTool
}

type Keyspace struct {
	ID                string `json:"key,omitempty"`
	Name              string `json:"name,omitempty"`
	Datacenter        string `json:"datacenter,omitempty"`
	ReplicationFactor int    `json:"replicationFactor,omitempty"`
	Contact           string `json:"contact,omitempty"`
	TTL               int    `json:"ttl,omitempty"`
	TUUID             bool   `json:"tuuid,omitempty"`
}

type KeyspaceResp struct {
	KSID string `json:"ksid"`
}

type KeyspaceEdit struct {
	Name    string
	Contact string
}

type MycenaePoints struct {
	Payload map[string]respPoints `json:"payload"`
}

type MycenaePointsText struct {
	Payload map[string]respPointsText `json:"payload"`
}

type respPoints struct {
	Points PayPoints `json:"points"`
}

type respPointsText struct {
	Points PayPoints `json:"text"`
}

type PayPoints struct {
	Count int             `json:"count"`
	Total int             `json:"total"`
	Ts    [][]interface{} `json:"ts"`
}

type Payload struct {
	Value     *float32          `json:"value,omitempty"`
	Text      *string           `json:"text,omitempty"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	TagKey    string            `json:"-"`
	TagValue  string            `json:"-"`
	TagKey2   string            `json:"-"`
	TagValue2 string            `json:"-"`
	Timestamp *int64            `json:"timestamp,omitempty"`
	Random    int               `json:"-"`
}

type PayloadSlice struct {
	PS []Payload
}

type MsgV2 struct {
	Value     float32           `json:"value,omitempty"`
	Text      string            `json:"text,omitempty"`
	Metric    string            `json:"metric,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
}

type RestErrors struct {
	Errors  []RestError `json:"errors"`
	Failed  int         `json:"failed"`
	Success int         `json:"success"`
}

type RestError struct {
	Datapoint *MsgV2 `json:"datapoint"`
	Error     string `json:"error"`
}

type Error struct {
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

type Point struct {
	Value     float32           `json:"value"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
}

type TextPoint struct {
	Text      string            `json:"text"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
}

type ResponseMeta struct {
	TotalRecord int      `json:"totalRecords"`
	Payload     []TsMeta `json:"payload"`
}

type TsMeta struct {
	TsID   string            `json:"id"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type ResponseQuery struct {
	Metric  string                 `json:"metric"`
	Tags    map[string]string      `json:"tags"`
	AggTags []string               `json:"aggregateTags"`
	Tsuuids []string               `json:"tsuids"`
	Dps     map[string]interface{} `json:"dps"`
	Query   *structs.TSDBquery     `json:"query,omitempty"`
}

type ResponseMetricTags struct {
	TotalRecords int      `json:"totalRecords,omitempty"`
	Payload      []string `json:"payload,omitempty"`
}

type TsError struct {
	ID      string
	Error   string
	Message string
	Date    time.Time
}

type TsErrorV2 struct {
	Metric string
	Tags   []TsTagV2 `json:"tagsError"`
}

type TsTagV2 struct {
	TagKey   string
	TagValue string
}

type TSDBqueryPayload struct {
	Relative string      `json:"relative"`
	Queries  []TSDBquery `json:"queries"`
}

type TSDBquery struct {
	Aggregator  string            `json:"aggregator"`
	Downsample  string            `json:"downsample"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags"`
	Rate        bool              `json:"rate"`
	RateOptions TSDBrateOptions   `json:"rateOptions"`
	Order       []string          `json:"order"`
	FilterValue string            `json:"filterValue"`
	Filters     []TSDBfilter      `json:"filters"`
}

type TSDBrateOptions struct {
	Counter    bool   `json:"counter"`
	CounterMax *int64 `json:"counterMax"`
	ResetValue int64  `json:"resetValue"`
}

type TSDBfilter struct {
	Ftype   string `json:"type"`
	Tagk    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

const MetricForm string = "testMetric-"
const TagKeyForm string = "testTagKey-"
const TagValueForm string = "testTagValue-"

var Sleep2 = 3 * time.Second
var Sleep3 = 5 * time.Second

func (m *mycenaeTool) Init(set MycenaeSettings) {
	ht := new(httpTool)
	ht.Init(set.Node, set.Port, set.Timeout)
	m.client = ht

	return
}

func (m *mycenaeTool) CreateKeyspace(dc, name, contact string, ttl, repFactor int) string {

	req := Keyspace{
		Datacenter:        dc,
		Name:              name,
		Contact:           contact,
		TTL:               ttl,
		ReplicationFactor: repFactor,
	}

	var resp *KeyspaceResp

	m.client.POSTjson(fmt.Sprintf("keyspaces/%s", name), req, &resp)

	return resp.KSID
}

func (m *mycenaeTool) GetPoints(keyspace string, start int64, end int64, id string) (int, MycenaePoints) {

	payload := `{
		"keys": [{
			"tsid":"` + id + `"
		}],
		"start":` + strconv.FormatInt(start * 1000, 10)  + `,
		"end":` + strconv.FormatInt(end* 1000, 10) + `
	}`

	status, resp, err := m.client.POST(fmt.Sprintf("keyspaces/%s/points", keyspace), []byte(payload))
	if err != nil {
		fmt.Println(err)
	}

	response := MycenaePoints{}

	if status == 200 {

		err = json.Unmarshal(resp, &response)
		if err != nil {
			fmt.Println(err)
		}
	}

	return status, response
}

func (m *mycenaeTool) GetTextPoints(keyspace string, start int64, end int64, id string) (int, MycenaePointsText) {

	payload := `{
		"text": [{
			"tsid":"` + id + `"
		}],
		"start":` + strconv.FormatInt(start*1000, 10) + `,
		"end":` + strconv.FormatInt(end*1000, 10) + `
	}`

	status, resp, err := m.client.POST(fmt.Sprintf("keyspaces/%s/points", keyspace), []byte(payload))
	if err != nil {
		fmt.Println(err)
	}

	response := MycenaePointsText{}

	if status == 200 {

		err = json.Unmarshal(resp, &response)
		if err != nil {
			fmt.Println(err)
		}
	}

	return status, response
}

func (m *mycenaeTool) GetPayload(keyspace string) *Payload {

	timestamp := time.Now().Unix()
	var value float32 = 5.1
	random := rand.Int()

	p := &Payload{
		Value:     &value,
		Metric:    fmt.Sprint(MetricForm, random),
		TagKey:    fmt.Sprint(TagKeyForm, random),
		TagValue:  fmt.Sprint(TagValueForm, random),
		Timestamp: &timestamp,
		Random:    random,
	}

	p.Tags = map[string]string{
		p.TagKey: p.TagValue,
		"ksid":   keyspace,
	}

	return p
}

func (m *mycenaeTool) GetTextPayload(keyspace string) *Payload {

	timestamp := time.Now().Unix()
	var value = "text ts text"
	random := rand.Int()

	p := &Payload{
		Text:      &value,
		Metric:    fmt.Sprint(MetricForm, random),
		TagKey:    fmt.Sprint(TagKeyForm, random),
		TagValue:  fmt.Sprint(TagValueForm, random),
		Timestamp: &timestamp,
		Random:    random,
	}

	p.Tags = map[string]string{
		p.TagKey: p.TagValue,
		"ksid":   keyspace,
	}

	return p
}

func (m *mycenaeTool) GetRandomMetricTags() (metric, tagKey, tagValue string, timestamp int64) {

	random := rand.Int()
	metric = fmt.Sprint("testMetric-", random)
	tagKey = fmt.Sprint("testTagKey-", random)
	tagValue = fmt.Sprint("testTagValue-", random)
	timestamp = time.Now().Unix()

	return
}

func (p Payload) Marshal() []byte {

	pByte, err := json.Marshal(p)
	if err != nil {
		fmt.Println(err)
	}

	return pByte
}

func (p PayloadSlice) Marshal() []byte {

	pByte, err := json.Marshal(p.PS)
	if err != nil {
		fmt.Println(err)
	}

	return pByte
}

func (p Payload) StringArray() string {

	str, err := json.Marshal(p)
	if err != nil {
		fmt.Println(err)
	}

	return fmt.Sprintf(`[%s]`, str)
}

func (k Keyspace) Marshal() []byte {

	body, err := json.Marshal(k)
	if err != nil {
		fmt.Println(err)
	}

	return body
}

func (ke KeyspaceEdit) Marshal() []byte {

	body, err := json.Marshal(ke)
	if err != nil {
		fmt.Println(err)
	}

	return body
}
