package structs

import (
	"regexp"

	"github.com/uol/gobol"
)

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

type FilterValueOperation struct {
	Enabled  bool
	BoolOper string
	Value    float64
}

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

func (query *TsQuery) GetRe() *regexp.Regexp {
	return query.re
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
			return errValidation(err)
		}
		query.re = re
	}

	return nil
}

type Key struct {
	TSid string `json:"tsid"`
	TTL  int    `json:"ttl"`
}

type Merge struct {
	Option string `json:"option"`
	Keys   []Key  `json:"keys"`
}
