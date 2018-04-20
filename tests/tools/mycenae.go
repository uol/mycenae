package tools

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"net/http"

	"github.com/uol/mycenae/lib/structs"
)

type mycenaeTool struct {
	client *httpTool
}

type Keyspace struct {
	Name              string `json:"name,omitempty"`
	Datacenter        string `json:"datacenter,omitempty"`
	ReplicationFactor int    `json:"replicationFactor,omitempty"`
	Contact           string `json:"contact,omitempty"`
	TTL               int    `json:"ttl,omitempty`
}

type KeyspaceUpdate struct {
	Contact string `json:"contact"`
}

type KeyspaceResp struct {
	KSID string `json:"ksid"`
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

//currently, there is no uses for this class in the current scylla code
/*type RestErrors struct {
	Errors  []RestError `json:"errors"`
	Failed  int         `json:"failed"`
	Success int         `json:"success"`
}*/

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

type LookupResult struct {
	TSUID  string            `json:"tsuid"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

type LookupResultObject struct {
	Type         string         `json:"type"`
	Metric       string         `json:"metric"`
	Tags         []string       `json:"tags,omitempty"`
	Limit        int            `json:"limit"`
	Time         int            `json:"time"`
	Results      []LookupResult `json:"results"`
	StartIndex   int            `json:"startIndex"`
	TotalResults int            `json:"totalResults"`
}

const MetricForm string = "testMetric-"
const TagKeyForm string = "testTagKey-"
const TagValueForm string = "testTagValue-"

var Sleep2 = 3 * time.Second
var Sleep3 = 5 * time.Second

func (m *mycenaeTool) Init(set RestAPISettings) {
	ht := new(httpTool)
	ht.Init(set.Node, set.Port, set.Timeout)
	m.client = ht

	return
}

func (m *mycenaeTool) CreateKeyspace(dc, name, contact string, repFactor int) string {

	req := Keyspace{
		Datacenter:        dc,
		Name:              name,
		Contact:           contact,
		ReplicationFactor: repFactor,
	}

	var resp *KeyspaceResp
	m.client.POSTjson(fmt.Sprintf("keysets/%s", name), req, &resp)

	return resp.KSID
}

func (m *mycenaeTool) CreateKeySet(name string) string {

	status, _, err := m.client.POST(fmt.Sprintf("keysets/%s", name), nil)

	if err != nil {
		panic(err)
	}

	if status != http.StatusCreated {
		panic("keyset creation failed with status: " + string(status))
	}

	fmt.Println("KeySet created:", name)

	return name
}

func (m *mycenaeTool) GetPoints(keyspace string, start int64, end int64, id string) (int, MycenaePoints) {

	payload := `{
		"keys": [{
			"tsid":"` + id + `",
			"ttl":1
		}],
		"start":` + strconv.FormatInt(start*1000, 10) + `,
		"end":` + strconv.FormatInt(end*1000, 10) + `
	}`

	status, resp, err := m.client.POST(fmt.Sprintf("keysets/%s/points", keyspace), []byte(payload))
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
			"tsid":"` + id + `",
			"ttl": 1
		}],
		"start":` + strconv.FormatInt(start*1000, 10) + `,
		"end":` + strconv.FormatInt(end*1000, 10) + `
	}`

	status, resp, err := m.client.POST(fmt.Sprintf("keysets/%s/points", keyspace), []byte(payload))
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
		"ttl":    "1",
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
		"ttl":    "1",
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

func (k KeyspaceUpdate) Marshal() []byte {

	body, err := json.Marshal(k)
	if err != nil {
		fmt.Println(err)
	}

	return body
}

func CreatePayload(v float32, m string, t map[string]string) Payload {

	return Payload{
		Value:  &v,
		Metric: m,
		Tags:   t,
	}
}

func CreatePayloadTS(v float32, m string, t map[string]string, ts int64) Payload {

	p := CreatePayload(v, m, t)
	p.Timestamp = &ts

	return p
}

func CreateTextPayload(txt string, m string, t map[string]string) Payload {

	return Payload{
		Text:   &txt,
		Metric: m,
		Tags:   t,
	}
}

func CreateTextPayloadTS(txt string, m string, t map[string]string, ts int64) Payload {

	p := CreateTextPayload(txt, m, t)
	p.Timestamp = &ts

	return p
}

func GetTSUIDFromPayload(payload *Payload, number bool) string {

	if number {
		return GetHashFromMetricAndTags(payload.Metric, payload.Tags)
	} else {
		return GetTextHashFromMetricAndTags(payload.Metric, payload.Tags)
	}
}
