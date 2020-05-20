package collector

import (
	"strings"

	"github.com/buger/jsonparser"
	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"
)

//
// Implements the JSON point parser used by the http and udp sources.
// @author: rnojiri
//

const (
	cMsgPointIgnored string = "points ignored"
)

// ParsePoints - parses an array of points
func (collect *Collector) ParsePoints(function string, isNumber bool, data []byte, outPoints *structs.TSDBpoints, outErrs *[]gobol.Error) string {

	defer func() {
		if err := recover(); err != nil {
			if logh.DebugEnabled {
				collect.logger.Debug().Str(constants.StringsFunc, function).Err(err.(error)).Msg(string(data))
			}
		}
	}()

	_, dtype, _, err := jsonparser.Get(data)
	if err != nil {
		(*outErrs) = append((*outErrs), validation.ErrMalformedJSON)
		return constants.StringsEmpty
	}

	var keyset string

	if dtype == jsonparser.Array {
		keyset = collect.ParsePointArray(function, isNumber, data, outPoints, outErrs)
	} else {
		var gerr gobol.Error
		var point *structs.TSDBpoint

		point, keyset, gerr = collect.ParsePoint(function, isNumber, data)
		if gerr != nil {
			(*outErrs) = append((*outErrs), gerr)
		}

		(*outPoints) = append((*outPoints), point)
	}

	return keyset
}

// ParsePointArray - parses an array of points
func (collect *Collector) ParsePointArray(function string, isNumber bool, data []byte, outPoints *structs.TSDBpoints, outErrs *[]gobol.Error) (keyset string) {

	_, err := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, inErr error) {

		if inErr != nil {
			(*outErrs) = append((*outErrs), validation.ErrMalformedJSON)
			return
		}

		var point *structs.TSDBpoint
		var gerr gobol.Error
		point, keyset, gerr = collect.ParsePoint(function, isNumber, value)
		if gerr != nil {
			(*outErrs) = append((*outErrs), gerr)
			if gerr == validation.ErrInvalidTTLValue || gerr == validation.ErrInexistentKeyset || gerr == validation.ErrInvalidKeysetFormat {
				panic(gerr)
			}

			return
		}

		(*outPoints) = append((*outPoints), point)
	})

	if err != nil {
		(*outErrs) = append((*outErrs), validation.ErrReadingJSONBytes)
	}

	return keyset
}

// ParsePoint - parses the json bytes to the object fields (seconds return is the keyset)
func (collect *Collector) ParsePoint(function string, isNumber bool, data []byte) (*structs.TSDBpoint, string, gobol.Error) {

	var err error
	var gerr gobol.Error

	p := structs.TSDBpoint{}
	p.Tags = []structs.TSDBTag{}
	ttlFound := false
	ksidFound := false

	var tagsError gobol.Error

	err = jsonparser.ObjectEach(data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {

		tag := structs.TSDBTag{}

		tag.Name, err = jsonparser.ParseString(key)
		if err != nil {
			tagsError = validation.ErrParsingTagKey
			return nil
		}

		tag.Value, err = jsonparser.ParseString(value)
		if err != nil {
			tagsError = validation.ErrParsingTagValue
			return nil
		}

		switch tag.Name {
		case constants.StringsTTL:
			ttl, ttlStr, gerr := collect.validation.ParseTTL(tag.Value)
			if gerr != nil {
				tagsError = gerr
				return nil
			}
			p.TTL = ttl
			tag.Value = ttlStr
			ttlFound = true
		case constants.StringsKSID:
			gerr = collect.validation.ValidateKeyset(tag.Value)
			if gerr != nil {
				tagsError = gerr
				return nil
			}
			p.Keyset = tag.Value
			ksidFound = true
		default:
			gerr = collect.validation.ValidateProperty(tag.Name, validation.TagKeyType)
			if gerr != nil {
				tagsError = gerr
				return nil
			}

			gerr = collect.validation.ValidateProperty(tag.Value, validation.TagValueType)
			if gerr != nil {
				tagsError = gerr
				return nil
			}
		}

		p.Tags = append(p.Tags, tag)

		return nil

	}, constants.StringsTags)

	if tagsError != nil {
		if gerr, ok := err.(gobol.Error); ok {
			return nil, p.Keyset, gerr
		}

		return nil, p.Keyset, tagsError
	}

	if !ttlFound {
		defaultTTLTag, defaultTTL := collect.validation.GetDefaultTTLTag()
		p.Tags = append(p.Tags, *defaultTTLTag)
		p.TTL = defaultTTL
	}

	if !ksidFound {
		return nil, p.Keyset, validation.ErrNoKeysetTag
	}

	gerr = collect.validation.ValidateTags(&p)
	if gerr != nil {
		return nil, p.Keyset, gerr
	}

	if p.Metric, err = jsonparser.GetString(data, constants.StringsMetric); err != nil {
		return nil, p.Keyset, validation.ErrParsingMetric
	}

	gerr = collect.validation.ValidateProperty(p.Metric, validation.MetricType)
	if gerr != nil {
		return nil, p.Keyset, gerr
	}

	if p.Timestamp, err = jsonparser.GetInt(data, constants.StringsTimestamp); err != nil && err != jsonparser.KeyPathNotFoundError {
		return nil, p.Keyset, validation.ErrParsingTimestamp
	}

	p.Timestamp, gerr = collect.validation.ValidateTimestamp(p.Timestamp)
	if gerr != nil {
		return nil, p.Keyset, gerr
	}

	if isNumber {
		dataIn, tdata, _, err := jsonparser.Get(data, constants.StringsValue)
		if err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, p.Keyset, validation.ErrParsingValue
		}

		switch tdata {
		case jsonparser.Number:
			value, err := jsonparser.ParseFloat(dataIn)
			if err != nil {
				return nil, p.Keyset, validation.ErrParsingValue
			}
			p.Value = &value
		case jsonparser.Null:
			p.Value = nil
		case jsonparser.NotExist:
			p.Value = nil
		default:
			return nil, p.Keyset, validation.ErrParsingValue
		}
	} else {
		if p.Text, err = jsonparser.GetString(data, constants.StringsText); err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, p.Keyset, validation.ErrParsingText
		}

		p.Text = strings.TrimSpace(p.Text)
	}

	gerr = collect.validation.ValidateType(&p, isNumber)
	if gerr != nil {
		return nil, p.Keyset, gerr
	}

	return &p, p.Keyset, nil
}
