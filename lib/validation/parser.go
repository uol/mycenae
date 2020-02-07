package validation

import (
	"github.com/buger/jsonparser"
	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"strings"
)

const (
	cFuncParsePoints string = "ParsePoints"
	cMsgParsePoints  string = "Error reading JSON bytes."
	cMsgPointIgnored string = "points ignored"
)

// ParsePoints - parses an array of points
func (v *Service) ParsePoints(function string, isNumber bool, data []byte, outPoints *structs.TSDBpoints, outErrs *[]gobol.Error) {

	defer func() {
		if err := recover(); err != nil {
			if logh.DebugEnabled {
				v.logger.Debug().Str(constants.StringsFunc, function).Err(err.(error)).Msg(string(data))
			}
		}
	}()

	_, dtype, _, err := jsonparser.Get(data)
	if err != nil {
		(*outErrs) = append((*outErrs), errMalformedJSON)
		return
	}

	if dtype == jsonparser.Array {
		v.ParsePointArray(function, isNumber, data, outPoints, outErrs)
	} else {
		point, gerr := v.ParsePoint(function, isNumber, data)
		if gerr != nil {
			(*outErrs) = append((*outErrs), gerr)
		}

		(*outPoints) = append((*outPoints), point)
	}
}

// ParsePointArray - parses an array of points
func (v *Service) ParsePointArray(function string, isNumber bool, data []byte, outPoints *structs.TSDBpoints, outErrs *[]gobol.Error) {

	_, err := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, inErr error) {

		if inErr != nil {
			(*outErrs) = append((*outErrs), errMalformedJSON)
			return
		}

		point, gerr := v.ParsePoint(function, isNumber, value)
		if gerr != nil {
			(*outErrs) = append((*outErrs), gerr)
			if gerr == errInvalidTTLValue || gerr == errInexistentKeyset || gerr == errInvalidKeysetFormat {
				panic(gerr)
			}

			return
		}

		(*outPoints) = append((*outPoints), point)
	})

	if err != nil {
		(*outErrs) = append((*outErrs), errBadRequest(cFuncParsePoints, cMsgParsePoints, err))
	}
}

// ParsePoint - parses the json bytes to the object fields
func (v *Service) ParsePoint(function string, isNumber bool, data []byte) (*structs.TSDBpoint, gobol.Error) {

	var err error
	var gerr gobol.Error
	p := structs.TSDBpoint{}

	if p.Metric, err = jsonparser.GetString(data, constants.StringsMetric); err != nil {
		return nil, errParsingMetric
	}

	gerr = v.ValidateProperty(p.Metric, MetricType)
	if gerr != nil {
		return nil, gerr
	}

	if p.Timestamp, err = jsonparser.GetInt(data, constants.StringsTimestamp); err != nil && err != jsonparser.KeyPathNotFoundError {
		return nil, errParsingTimestamp
	}

	p.Timestamp, gerr = v.ValidateTimestamp(p.Timestamp)
	if gerr != nil {
		return nil, gerr
	}

	if isNumber {
		dataIn, tdata, _, err := jsonparser.Get(data, constants.StringsValue)
		if err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, errParsingValue
		}

		switch tdata {
		case jsonparser.Number:
			value, err := jsonparser.ParseFloat(dataIn)
			if err != nil {
				return nil, errParsingValue
			}
			p.Value = &value
		case jsonparser.Null:
			p.Value = nil
		case jsonparser.NotExist:
			p.Value = nil
		default:
			return nil, errParsingValue
		}
	} else {
		if p.Text, err = jsonparser.GetString(data, constants.StringsText); err != nil && err != jsonparser.KeyPathNotFoundError {
			return nil, errParsingText
		}

		p.Text = strings.TrimSpace(p.Text)
	}

	gerr = v.ValidateType(&p, isNumber)
	if gerr != nil {
		return nil, gerr
	}

	p.Tags = []structs.TSDBTag{}
	ttlFound := false
	ksidFound := false
	err = jsonparser.ObjectEach(data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {

		tag := structs.TSDBTag{}

		tag.Name, err = jsonparser.ParseString(key)
		if err != nil {
			return errParsingTagKey
		}

		tag.Value, err = jsonparser.ParseString(value)
		if err != nil {
			return errParsingTagValue
		}

		switch tag.Name {
		case constants.StringsTTL:
			ttl, ttlStr, gerr := v.ParseTTL(tag.Value)
			if gerr != nil {
				return gerr
			}
			p.TTL = ttl
			tag.Value = ttlStr
			ttlFound = true
		case constants.StringsKSID:
			gerr = v.ValidateKeyset(tag.Value)
			if gerr != nil {
				return gerr
			}
			p.Keyset = tag.Value
			ksidFound = true
		default:
			gerr = v.ValidateProperty(tag.Name, TagKeyType)
			if gerr != nil {
				return gerr
			}

			gerr = v.ValidateProperty(tag.Value, TagValueType)
			if gerr != nil {
				return gerr
			}
		}

		dup := false
		for i, k := range p.Tags {
			if k.Name == tag.Name {
				p.Tags[i].Value = tag.Value
				dup = true
				break
			}
		}

		if !dup {
			p.Tags = append(p.Tags, tag)
		}

		return nil

	}, constants.StringsTags)

	if err != nil {
		if gerr, ok := err.(gobol.Error); ok {
			return nil, gerr
		}

		return nil, errMalformedJSON
	}

	if !ttlFound {
		p.Tags = append(p.Tags, v.defaultTTLTag)
		p.TTL = v.configuration.DefaultTTL
	}

	if !ksidFound {
		return nil, errNoKeysetTag
	}

	gerr = v.ValidateTags(&p)
	if gerr != nil {
		return nil, gerr
	}

	return &p, nil
}
