package validation

import (
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

//
// Validation errors.
// @author: rnojiri
//

const (
	cPackage string = "validation"
)

// errBadRequest - bad request error
func errBadRequest(function, message string, err error) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			cPackage,
			function,
			message,
			http.StatusBadRequest,
		)
	}
	return nil
}

// errSimpleBadRequest - bad request error without error
func errSimpleBadRequest(function, message string) gobol.Error {
	return errBadRequest(function, message, fmt.Errorf(message))
}

var (
	errParsingMetric       = errSimpleBadRequest("ParsePoint", `Error parsing "metric" from JSON.`)
	errParsingTimestamp    = errSimpleBadRequest("ParsePoint", `Error parsing "timestamp" from JSON.`)
	errParsingValue        = errSimpleBadRequest("ParsePoint", `Error parsing "value" from JSON.`)
	errParsingText         = errSimpleBadRequest("ParsePoint", `Error parsing "text" from JSON.`)
	errParsingTagKey       = errSimpleBadRequest("ParsePoint", `Error parsing tag key from JSON.`)
	errParsingTagValue     = errSimpleBadRequest("ParsePoint", `Error parsing tag value from JSON.`)
	errNumberTypeExpected  = errSimpleBadRequest("ValidateType", `Wrong Format: Field "value" is required.`)
	errTextTypeExpected    = errSimpleBadRequest("ValidateType", `Wrong Format: Field "text" is required.`)
	errMaxTextValueSize    = errSimpleBadRequest("ValidateType", `Wrong Format: Field "text" has exceeded the maximum number of characters allowed`)
	errNoTags              = errSimpleBadRequest("ValidateTags", `Wrong Format: At least one tag is required.`)
	errNoUserTags          = errSimpleBadRequest("ValidateTags", `Wrong Format: At least one tag other than "ksid" and "ttl" is required.`)
	errNoKeysetTag         = errSimpleBadRequest("ValidateKeyset", `Wrong Format: Tag "ksid" is required.`)
	errInvalidKeysetFormat = errSimpleBadRequest("ValidateKeyset", `Wrong Format: Field "ksid" has a invalid format.`)
	errInvalidTagKey       = errSimpleBadRequest("ValidateProperty", `Wrong Format: Tag key has a invalid format.`)
	errInvalidTagValue     = errSimpleBadRequest("ValidateProperty", `Wrong Format: Tag value has a invalid format.`)
	errInvalidMetric       = errSimpleBadRequest("ValidateProperty", `Wrong Format: Field "metric" (%s) is not well formed.`)
	errInvalidPropertyType = errSimpleBadRequest("ValidateProperty", `Property type is not mapped`)
	errInvalidTTLValue     = errSimpleBadRequest("ParseTTL", `Wrong Format: Tag "ttl" must be a positive number.`)
	errInexistentKeyset    = errSimpleBadRequest("ValidateKeyset", `Keyset not exists.`)
	errMalformedJSON       = errSimpleBadRequest("ParsePoint", `JSON is malformed.`)
)
