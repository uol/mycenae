package collector

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/buger/jsonparser"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tserr"
)

// TSDBpoints - an array of point
type TSDBpoints []*TSDBpoint

// ParsePoints - parses an array of points
func ParsePoints(function string, isNumber bool, data []byte) (TSDBpoints, gobol.Error) {

	var gerr gobol.Error
	points := TSDBpoints{}

	dataIn, dtype, _, err := jsonparser.Get(data)
	if err != nil {
		return nil, errUnmarshal(function, err)
	}

	switch dtype {
	case jsonparser.Array:
		_, err := jsonparser.ArrayEach(dataIn, func(value []byte, dataType jsonparser.ValueType, offset int, inErr error) {

			if gerr != nil {
				return
			}

			if inErr != nil {
				gerr = errUnmarshal(function, inErr)
				return
			}

			point, err := ParsePoint(function, isNumber, value)
			if err != nil {
				gerr = errUnmarshal(function, err)
				return
			}

			points = append(points, point)
		})

		if err != nil {
			gerr = errUnmarshal(function, err)
		}

	case jsonparser.Object:

		point, err := ParsePoint(function, isNumber, data)
		if err != nil {
			return nil, errUnmarshal(function, err)
		}

		points = append(points, point)
	}

	return points, gerr
}

// TSDBpoint - an opentsdb point
type TSDBpoint struct {
	Metric    string            `json:"metric,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	Value     *float64          `json:"value,omitempty"`
	Text      string            `json:"text,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// ParsePoint - parses the json bytes to the object fields
func ParsePoint(function string, isNumber bool, data []byte) (*TSDBpoint, gobol.Error) {

	var err error
	p := &TSDBpoint{}

	if p.Metric, err = jsonparser.GetString(data, "metric"); err != nil {
		return nil, errUnmarshal(function, err)
	}

	if p.Timestamp, err = jsonparser.GetInt(data, "timestamp"); err != nil && err != jsonparser.KeyPathNotFoundError {
		return nil, errUnmarshal(function, err)
	}

	if isNumber {

		dataIn, tdata, _, err := jsonparser.Get(data, "value")
		if err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, errUnmarshal(function, err)
		}

		switch tdata {
		case jsonparser.Number:
			value, err := jsonparser.ParseFloat(dataIn)
			if err != nil {
				return nil, errUnmarshal(function, err)
			}
			p.Value = &value
		case jsonparser.Null:
			p.Value = nil
		case jsonparser.NotExist:
			p.Value = nil
		default:
			return nil, errUnmarshal(function, fmt.Errorf("error parsing value from point"))
		}

	} else {

		if p.Text, err = jsonparser.GetString(data, "text"); err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, errUnmarshal(function, err)
		}

		if p.Text == constants.StringsEmpty {
			return nil, errBadRequest(function, "text cannot be empty", fmt.Errorf("point's text cannot be empty"))
		}
	}

	p.Tags = map[string]string{}
	err = jsonparser.ObjectEach(data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {

		tagKey, err := jsonparser.ParseString(key)
		if err != nil {
			return errUnmarshal(function, err)
		}

		if p.Tags[tagKey], err = jsonparser.ParseString(value); err != nil {
			return errUnmarshal(function, err)
		}

		return nil

	}, "tags")

	return p, errUnmarshal(function, err)
}

const (
	cNoPointsMsg  string = "no points"
	cFuncValidate string = "Validate"
)

var (
	errNoPoints = errors.New(cNoPointsMsg)
)

// Validate - validates the array of points
func (p TSDBpoints) Validate() gobol.Error {
	if len(p) == 0 {
		return tserr.New(
			errNoPoints,
			cNoPointsMsg,
			cPackage,
			cFuncValidate,
			http.StatusBadRequest,
		)
	}
	return nil
}

type RestError struct {
	Datapoint TSDBpoint   `json:"datapoint"`
	Gerr      gobol.Error `json:"error"`
}

type RestErrorUser struct {
	Datapoint TSDBpoint   `json:"datapoint"`
	Error     interface{} `json:"error"`
}

type RestErrors struct {
	Errors  []RestErrorUser `json:"errors"`
	Failed  int             `json:"failed"`
	Success int             `json:"success"`
}

type Point struct {
	Message   *TSDBpoint
	ID        string
	Keyset    string
	Timestamp int64
	Number    bool
	TTL       int
}

type StructV2Error struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tagsError"`
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

type LogMeta struct {
	Action string   `json:"action"`
	Meta   MetaInfo `json:"meta"`
}

type EsIndex struct {
	EsID    string `json:"_id"`
	EsType  string `json:"_type"`
	EsIndex string `json:"_index"`
}

type BulkType struct {
	ID EsIndex `json:"index"`
}

type EsMetric struct {
	Metric string `json:"metric"`
}

type EsTagKey struct {
	Key string `json:"key"`
}

type EsTagValue struct {
	Value string `json:"value"`
}
