package plot

import (
	"regexp"
	"sort"
	//"fmt"
	//"strings"
	//"strconv"

	"github.com/uol/gobol"
	//"github.com/uol/mycenae/lib/config"
	//"github.com/uol/mycenae/lib/structs"
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
	index int
	Count int
	Total int
	Data  Pnts
	gerr  gobol.Error
}

type TST struct {
	index int
	Count int
	Total int
	Data  TextPnts
	gerr  gobol.Error
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

type EsResponseTag struct {
	Took     int                  `json:"took"`
	TimedOut bool                 `json:"timed_out"`
	Shards   EsRespShards         `json:"_shards"`
	Hits     EsRespHitsWrapperTag `json:"hits"`
}

type EsRespShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type EsRespHitsWrapperTag struct {
	Total    int             `json:"total"`
	MaxScore float32         `json:"max_score"`
	Hits     []EsRespHitsTag `json:"hits"`
}

type EsRespHitsTag struct {
	Index   string  `json:"_index"`
	Type    string  `json:"_type"`
	Id      string  `json:"_id"`
	Version int     `json:"_version,omitempty"`
	Found   bool    `json:"found,omitempty"`
	Score   float32 `json:"_score"`
	Source  TagKey  `json:"_source"`
}

type TagKey struct {
	Key string `json:"key"`
}

type EsResponseMetric struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   EsRespShards            `json:"_shards"`
	Hits     EsRespHitsWrapperMetric `json:"hits"`
}

type EsRespHitsWrapperMetric struct {
	Total    int                `json:"total"`
	MaxScore float32            `json:"max_score"`
	Hits     []EsRespHitsMetric `json:"hits"`
}

type EsRespHitsMetric struct {
	Index   string     `json:"_index"`
	Type    string     `json:"_type"`
	Id      string     `json:"_id"`
	Version int        `json:"_version,omitempty"`
	Found   bool       `json:"found,omitempty"`
	Score   float32    `json:"_score"`
	Source  MetricName `json:"_source"`
}

type MetricName struct {
	Name string `json:"name"`
}

type EsResponseTagKey struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   EsRespShards            `json:"_shards"`
	Hits     EsRespHitsWrapperTagKey `json:"hits"`
}

type EsRespHitsWrapperTagKey struct {
	Total    int                `json:"total"`
	MaxScore float32            `json:"max_score"`
	Hits     []EsRespHitsTagKey `json:"hits"`
}

type EsRespHitsTagKey struct {
	Index   string  `json:"_index"`
	Type    string  `json:"_type"`
	Id      string  `json:"_id"`
	Version int     `json:"_version,omitempty"`
	Found   bool    `json:"found,omitempty"`
	Score   float32 `json:"_score"`
	Source  TagKey  `json:"_source"`
}

type EsResponseTagValue struct {
	Took     int                       `json:"took"`
	TimedOut bool                      `json:"timed_out"`
	Shards   EsRespShards              `json:"_shards"`
	Hits     EsRespHitsWrapperTagValue `json:"hits"`
}

type EsRespHitsWrapperTagValue struct {
	Total    int                  `json:"total"`
	MaxScore float32              `json:"max_score"`
	Hits     []EsRespHitsTagValue `json:"hits"`
}

type EsRespHitsTagValue struct {
	Index   string   `json:"_index"`
	Type    string   `json:"_type"`
	Id      string   `json:"_id"`
	Version int      `json:"_version,omitempty"`
	Found   bool     `json:"found,omitempty"`
	Score   float32  `json:"_score"`
	Source  TagValue `json:"_source"`
}

type TagValue struct {
	Value string `json:"value"`
}

type EsResponseMeta struct {
	Took     int                   `json:"took"`
	TimedOut bool                  `json:"timed_out"`
	Shards   EsRespShards          `json:"_shards"`
	Hits     EsRespHitsWrapperMeta `json:"hits"`
}

type EsRespHitsWrapperMeta struct {
	Total    int              `json:"total"`
	MaxScore float32          `json:"max_score"`
	Hits     []EsRespHitsMeta `json:"hits"`
}

type EsRespHitsMeta struct {
	Index   string   `json:"_index"`
	Type    string   `json:"_type"`
	Id      string   `json:"_id"`
	Version int      `json:"_version,omitempty"`
	Found   bool     `json:"found,omitempty"`
	Score   float32  `json:"_score"`
	Source  MetaInfo `json:"_source"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

type QueryWrapper struct {
	Size   int64       `json:"size,omitempty"`
	From   int64       `json:"from,omitempty"`
	Query  BoolWrapper `json:"filter"`
	Fields []string    `json:"fields,omitempty"`
}

type BoolWrapper struct {
	Bool OperatorWrapper `json:"bool"`
}

type OperatorWrapper struct {
	Must    []interface{} `json:"must,omitempty"`
	MustNot []interface{} `json:"must_not,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

type EsRegexp struct {
	Regexp map[string]string `json:"regexp"`
}

type TsMetaInfo struct {
	TsId   string            `json:"id"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type EsNestedQuery struct {
	Nested EsNested `json:"nested"`
}

type EsNested struct {
	Path      string      `json:"path"`
	ScoreMode string      `json:"score_mode,omitempty"`
	Query     BoolWrapper `json:"filter"`
}

type Term struct {
	Term map[string]string `json:"term"`
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

/*
type TSDBquery struct {
	Aggregator  string            `json:"aggregator"`
	Downsample  string            `json:"downsample,omitempty"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags"`
	Rate        bool              `json:"rate,omitempty"`
	RateOptions TSDBrateOptions   `json:"rateOptions,omitempty"`
	Order       []string          `json:"order,omitempty"`
	FilterValue string            `json:"filterValue,omitempty"`
	Filters     []TSDBfilter      `json:"filters,omitempty"`
}
*/
/*
type TSDBqueryPayload struct {
	Start        int64       `json:"start,omitempty"`
	End          int64       `json:"end,omitempty"`
	Relative     string      `json:"relative,omitempty"`
	Queries      []structs.TSDBquery `json:"queries"`
	ShowTSUIDs   bool        `json:"showTSUIDs"`
	MsResolution bool        `json:"msResolution"`
}
*/
/*
func (query TSDBqueryPayload) Validate() gobol.Error {

	if query.Relative != "" {
		if err := query.checkDuration(query.Relative); err != nil {
			return err
		}
	}

	if len(query.Queries) == 0 {
		return errTSDBquery(fmt.Errorf("At least one quey should be present"))
	}

	for i, q := range query.Queries {

		if err := query.checkField("metric", q.Metric); err != nil {
			return err
		}

		if err := query.checkAggregator(q.Aggregator); err != nil {
			return err
		}

		if q.Downsample != "" {

			ds := strings.Split(q.Downsample, "-")

			if len(ds) < 2 {
				return errTSDBquery(fmt.Errorf("invalid downsample format"))
			}

			if err := query.checkDuration(ds[0]); err != nil {
				return err
			}

			if err := query.checkDownsampler(ds[1]); err != nil {
				return err
			}

			if len(ds) > 2 {
				if err := query.checkFiller(ds[2]); err != nil {
					return err
				}
			}

		}

		if q.Rate {
			if err := query.checkRate(q.RateOptions); err != nil {
				return err
			}
		}

		if q.FilterValue != "" {
			q.FilterValue = strings.Replace(q.FilterValue, " ", "", -1)
			query.Queries[i].FilterValue = q.FilterValue

			if len(q.FilterValue) < 2 {
				return errTSDBquery(fmt.Errorf("invalid filter value %s", q.FilterValue))
			}

			if q.FilterValue[:2] == ">=" || q.FilterValue[:2] == "<=" || q.FilterValue[:2] == "==" {
				_, err := strconv.ParseFloat(q.FilterValue[2:], 64)
				if err != nil {
					return errTSDBquery(err)
				}
			} else if q.FilterValue[:1] == ">" || q.FilterValue[:1] == "<" {
				_, err := strconv.ParseFloat(q.FilterValue[1:], 64)
				if err != nil {
					return errTSDBquery(err)
				}
			} else {
				return errTSDBquery(fmt.Errorf("invalid filter value %s", q.FilterValue))
			}
		}

		if len(q.Order) == 0 {

			if q.FilterValue != "" {
				query.Queries[i].Order = append(query.Queries[i].Order, "filterValue")
			}

			if q.Downsample != "" {
				query.Queries[i].Order = append(query.Queries[i].Order, "downsample")
			}

			query.Queries[i].Order = append(query.Queries[i].Order, "aggregation")

			if q.Rate {
				query.Queries[i].Order = append(query.Queries[i].Order, "rate")
			}

		} else {

			orderCheck := make([]string, len(q.Order))

			copy(orderCheck, q.Order)

			k := 0
			occur := 0
			for j, order := range orderCheck {

				if order == "aggregation" {
					k = j
					occur++
				}

			}

			if occur == 0 {
				return errTSDBquery(
					fmt.Errorf("aggregation configured but no aggregation found in order array"),
				)
			}

			if occur > 1 {
				return errTSDBquery(fmt.Errorf("more than one aggregation found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "filterValue" {
					k = j
					occur++
				}

			}

			if q.FilterValue != "" && occur == 0 {
				return errTSDBquery(
					fmt.Errorf("filterValue configured but no filterValue found in order array"),
				)
			}

			if occur > 1 {
				return errTSDBquery(fmt.Errorf("more than one filterValue found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "downsample" {
					k = j
					occur++
				}

			}

			if q.Downsample != "" && occur == 0 {
				return errTSDBquery(
					fmt.Errorf(
						"downsample configured but no downsample found in order array",
					),
				)
			}

			if occur > 1 {
				return errTSDBquery(fmt.Errorf("more than one downsample found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "rate" {
					k = j
					occur++
				}

			}

			if q.Rate && occur == 0 {
				return errTSDBquery(fmt.Errorf("rate configured but no rate found in order array"))
			}

			if occur > 1 {
				return errTSDBquery(fmt.Errorf("more than one rate found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			if len(orderCheck) != 0 {
				return errTSDBquery(fmt.Errorf("invalid operations in order array %v", orderCheck))
			}

		}

		if err := query.checkFilter(q.Filters); err != nil {
			return err
		}

	}

	return nil
}

func (query TSDBqueryPayload) checkRate(opts TSDBrateOptions) gobol.Error {

	if opts.CounterMax != nil && *opts.CounterMax < 0 {
		return errRate("counter max needs to be a positive integer")
	}

	return nil
}

func (query TSDBqueryPayload) checkAggregator(aggr string) gobol.Error {

	ok := false

	for _, vAggr := range config.GetAggregators() {
		if vAggr == aggr {
			ok = true
			break
		}
	}

	if !ok {
		return errAggregator("unkown aggregation value")
	}

	return nil
}

func (query TSDBqueryPayload) checkDownsampler(DSr string) gobol.Error {

	ok := false

	for _, vDSr := range config.GetDownsamplers() {
		if vDSr == DSr {
			ok = true
			break
		}
	}

	if !ok {
		return errDownsampler("Invalid downsample")
	}

	return nil
}

func (query TSDBqueryPayload) checkFiller(DSf string) gobol.Error {

	ok := false

	for _, vDSf := range config.GetFillers() {
		if vDSf == DSf {
			ok = true
			break
		}
	}

	if !ok {
		return errFiller("Invalid fill value")
	}

	return nil
}

func (query TSDBqueryPayload) checkFilter(filters []TSDBfilter) gobol.Error {

	vFilters := config.GetFilters()

	for _, filter := range filters {

		ok := false

		ft := filter.Ftype

		if ft == "iliteral_or" {
			ft = "literal_or"
		} else if ft == "not_iliteral_or" {
			ft = "not_literal_or"
		} else if ft == "iwildcard" {
			ft = "wildcard"
		}

		for _, vFilter := range vFilters {
			if ft == vFilter {
				ok = true
				break
			}
		}
		if !ok {
			return errFilter(fmt.Sprintf("Invalid filter type %s", filter.Ftype))
		}

		if err := query.checkField("tagk", filter.Tagk); err != nil {
			return err
		}

		if err := query.checkFilterField("filter", ft, filter.Filter); err != nil {
			return err
		}
	}

	return nil
}

func (query TSDBqueryPayload) checkDuration(s string) gobol.Error {

	if len(s) < 2 {
		return errCheckDuration(fmt.Errorf("Invalid time interval"))
	}

	var n int
	var err error

	if string(s[len(s)-2:]) == "ms" {
		n, err = strconv.Atoi(string(s[:len(s)-2]))
		if err != nil {
			return errCheckDuration(err)
		}
		return nil
	}

	switch s[len(s)-1:] {
	case "s", "m", "h", "d", "w", "n", "y":
		n, err = strconv.Atoi(string(s[:len(s)-1]))
		if err != nil {
			return errCheckDuration(err)
		}
	default:
		return errCheckDuration(fmt.Errorf("Invalid unit"))
	}

	if n < 1 {
		return errCheckDuration(fmt.Errorf("interval needs to be bigger than 0"))
	}

	return nil
}

func (query TSDBqueryPayload) checkField(n, f string) gobol.Error {

	if !validFields.MatchString(f) {
		return errField(fmt.Sprintf("Invalid characters in field %s: %s", n, f))
	}
	return nil
}

func (query TSDBqueryPayload) checkFilterField(n, tf, f string) gobol.Error {

	match := false

	switch tf {
	case "wildcard":
		match = validFwild.MatchString(f)
	case "literal_or", "not_literal_or":
		match = validFor.MatchString(f)
	case "regexp":
		match = true
	}

	if !match {
		return errFilterField(fmt.Sprintf("Invalid characters in field %s: %s", n, f))
	}

	return nil
}
*/
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
