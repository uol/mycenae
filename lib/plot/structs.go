package plot

import (
	"regexp"
	"sort"

	"github.com/uol/gobol"
)

var (
	validFields = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/]+$`)
	validFwild  = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/*]+$`)
	validFor    = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/|]+$`)
)

type TsQuery struct {
	Downsample Downsample       `json:"downsample"`
	Start      int64            `json:"start"`
	End        int64            `json:"end"`
	Keys       []Key            `json:"keys"`
	Merge      map[string]Merge `json:"merge"`
	Text       []Key            `json:"text"`
	TextSearch string           `json:"textSearch"`

	re *regexp.Regexp
}

func (query *TsQuery) Validate() gobol.Error {

	if query.End < query.Start {
		return errValidationS("ListPoints", "end date should be equal or bigger than start date")
	}

	if len(query.Merge) > 0 {

		for _, ks := range query.Merge {
			if len(ks.Keys) < 2 {
				return errValidationS(
					"ListPoints",
					"At least two different timeseries are required to create a merged one",
				)
			}
			t := 0
			for _, k := range ks.Keys {
				if k.TSid == "" {
					return errValidationS("ListPoints", "tsid cannot be empty")
				}
				if k.TSid[:1] == "T" {
					t++
				}
			}
			if t > 0 {
				if len(ks.Keys) != t {
					return errValidationS(
						"ListPoints",
						"Cannot merge number series with text series. Please group series by type",
					)
				}
			}
		}
	}

	if len(query.Merge) == 0 && len(query.Keys) == 0 && len(query.Text) == 0 {
		return errValidationS(
			"ListPoints",
			"No IDs found. At least one key or one text or one merge needs to be present",
		)
	}

	if query.Downsample.Enabled {
		if query.Downsample.Options.Downsample != "avg" &&
			query.Downsample.Options.Downsample != "max" &&
			query.Downsample.Options.Downsample != "min" &&
			query.Downsample.Options.Downsample != "sum" &&
			query.Downsample.Options.Downsample != "pnt" {

			return errValidationS(
				"ListPoints",
				"valid approximation values are 'avg' 'sum' 'max' 'min' 'ptn'",
			)
		}
	}

	for _, k := range query.Keys {
		if k.TSid[:1] == "T" {
			return errValidationS(
				"ListPoints",
				"key array does no support text keys, text keys should be in the text array",
			)
		}
	}

	if query.TextSearch != "" {
		re, err := regexp.Compile(query.TextSearch)
		if err != nil {
			return errValidation(
				"ListPoints",
				"invalid regular expression at textSearch",
				err,
			)
		}
		query.re = re
	}

	return nil
}

type Downsample struct {
	Enabled     bool      `json:"enabled"`
	PointLimit  bool      `json:"pointLimit"`
	TotalPoints int       `json:"totalPoints"`
	Options     DSoptions `json:"options"`
}

type DSoptions struct {
	Downsample string `json:"approximation"`
	Unit       string `json:"unit"`
	Value      int    `json:"value"`
	Fill       string
}

type Key struct {
	TSid string `json:"tsid"`
}

type Merge struct {
	Option string `json:"option"`
	Keys   []Key  `json:"keys"`
}

type Series struct {
	Text   interface{} `json:"text,omitempty"`
	Trend  interface{} `json:"trend,omitempty"`
	Points interface{} `json:"points,omitempty"`
}

type DataOperations struct {
	Downsample  Downsample
	Merge       string
	Rate        RateOperation
	Order       []string
	FilterValue FilterValueOperation
}

type RateOperation struct {
	Enabled bool
	Options TSDBrateOptions
}

type TSDBrateOptions struct {
	Counter    bool   `json:"counter"`
	CounterMax *int64 `json:"counterMax,omitempty"`
	ResetValue int64  `json:"resetValue,omitempty"`
}

type FilterValueOperation struct {
	Enabled  bool
	BoolOper string
	Value    float64
}

type SeriesType struct {
	Count int         `json:"count"`
	Total int         `json:"total"`
	Type  string      `json:"type,omitempty"`
	Ts    interface{} `json:"ts"`
}

type TSmeta struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tags"`
}

func (tsm TSmeta) Validate() gobol.Error {
	return nil
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}

type TS struct {
	Count int
	Total int
	Data  Pnts
}

type TST struct {
	Count int
	Total int
	Data  TextPnts
}

type Pnt struct {
	Date  int64
	Value float64
	Empty bool
}

type TextPnt struct {
	Date  int64  `json:"x"`
	Value string `json:"title"`
}

type Pnts []Pnt

func (s Pnts) Len() int {
	return len(s)
}

func (s Pnts) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func (s Pnts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type TextPnts []TextPnt

func (s TextPnts) Len() int {
	return len(s)
}

func (s TextPnts) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func (s TextPnts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type TagKey struct {
	Key string `json:"key"`
}

type MetricName struct {
	Name string `json:"name"`
}

type TagValue struct {
	Value string `json:"value"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

type TsMetaInfo struct {
	TsId   string            `json:"id"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type TSDBfilter struct {
	Ftype   string `json:"type"`
	Tagk    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

type TSDBobj struct {
	Tsuid  string            `json:"tsuid"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

type TSDBlookup struct {
	Type         string    `json:"type"`
	Metric       string    `json:"metric"`
	Tags         []Tag     `json:"tags"`
	Limit        int       `json:"limit"`
	Time         int       `json:"time"`
	Results      []TSDBobj `json:"results"`
	StartIndex   int       `json:"startIndex"`
	TotalResults int       `json:"totalResults"`
}

type TSDBresponses []TSDBresponse

func (r TSDBresponses) Len() int {
	return len(r)
}

func (r TSDBresponses) Less(i, j int) bool {

	if r[i].Metric != r[j].Metric {
		return r[i].Metric < r[j].Metric
	}

	keys := []string{}

	for k, _ := range r[i].Tags {
		keys = append(keys, k)
	}

	for kj, _ := range r[j].Tags {

		add := true

		for _, k := range keys {
			if k == kj {
				add = false
			}
		}

		if add {
			keys = append(keys, kj)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		if vi, ok := r[i].Tags[k]; ok {
			if vj, ok := r[j].Tags[k]; ok {
				if vi == vj {
					continue
				}
				return vi < vj
			} else {
				return true
			}
		} else {
			return false
		}
	}

	sort.Strings(r[i].AggregatedTags)
	sort.Strings(r[j].AggregatedTags)

	for _, ati := range r[i].AggregatedTags {
		for _, atj := range r[j].AggregatedTags {
			if ati == atj {
				break
			}
			return ati < atj
		}
	}

	return false
}

func (r TSDBresponses) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type TSDBresponse struct {
	Metric         string                 `json:"metric"`
	Tags           map[string]string      `json:"tags"`
	AggregatedTags []string               `json:"aggregateTags"`
	Tsuids         []string               `json:"tsuids,omitempty"`
	Dps            map[string]interface{} `json:"dps"`
}

type ExpParse struct {
	Expression string `json:"expression"`
	Expand     bool   `json:"expand"`
	Keyset     string `json:"ksid"`
}

func (expp ExpParse) Validate() gobol.Error {
	return nil
}

type ExpQuery struct {
	Expression string `json:"expression"`
}

func (eq ExpQuery) Validate() gobol.Error {
	return nil
}
