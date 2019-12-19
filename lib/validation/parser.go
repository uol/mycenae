package validation

import (
	"github.com/buger/jsonparser"
	"github.com/uol/gobol"
	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

const (
	cFuncParsePoints string = "ParsePoints"
	cMsgParsePoints  string = "Error reading JSON bytes."
	cMsgPointIgnored string = "points ignored"
)

// ParsePoints - parses an array of points
func (v *Service) ParsePoints(function string, isNumber bool, data []byte) (structs.TSDBpoints, []gobol.Error) {

	finalGerrs := []gobol.Error{}
	points := structs.TSDBpoints{}

	defer func() {
		if err := recover(); err != nil {
			if logh.DebugEnabled {
				v.logger.Debug().Str(constants.StringsFunc, function).Err(err.(error)).Msg(string(data))
			}
		}
	}()

	_, err := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, inErr error) {

		if inErr != nil {
			return
		}

		point, gerr := v.ParsePoint(function, isNumber, value)
		if gerr != nil {
			finalGerrs = append(finalGerrs, gerr)
			if gerr == errInvalidTTLValue || gerr == errInexistentKeyset || gerr == errInvalidKeysetFormat {
				panic(gerr)
			}

			return
		}

		points = append(points, point)
	})

	if err != nil {
		finalGerrs = append(finalGerrs, errBadRequest(cFuncParsePoints, cMsgParsePoints, err))
	}

	if len(finalGerrs) > 0 {
		return points, finalGerrs
	}

	return points, nil
}

// ParsePoint - parses the json bytes to the object fields
func (v *Service) ParsePoint(function string, isNumber bool, data []byte) (*structs.TSDBpoint, gobol.Error) {

	var err error
	var gerr gobol.Error
	p := structs.TSDBpoint{}

	if p.Metric, err = jsonparser.GetString(data, constants.StringsMetric); err != nil {
		return nil, errParsingMetric
	}

	gerr = v.ValidateProperty(p.Metric, metricType)
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
	}

	gerr = v.ValidateType(&p, isNumber)
	if gerr != nil {
		return nil, gerr
	}

	p.Tags = []structs.TSDBTag{}
	ttlFound := false
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
		default:
			gerr = v.ValidateProperty(tag.Name, tagKeyType)
			if gerr != nil {
				return gerr
			}

			gerr = v.ValidateProperty(tag.Value, tagValueType)
			if gerr != nil {
				return gerr
			}
		}

		p.Tags = append(p.Tags, tag)

		return nil

	}, constants.StringsTags)

	if err != nil {
		return nil, err.(gobol.Error)
	}

	if !ttlFound {
		p.Tags = append(p.Tags, v.defaultTTLTag)
	}

	gerr = v.ValidateTags(&p)
	if gerr != nil {
		return nil, gerr
	}

	return &p, nil
}
