package validation

import (
	"fmt"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

//
// Pre built validation errors.
// @author: rnojiri
//

const (
	cMakePacket  string = "makePacket"
	cWrongFormat string = "Wrong JSON format"
	cPackage     string = "collector"
)

// NewValidationError - creates a new validation error
func NewValidationError(function, message string, err error, errCode string) gobol.Error {
	return tserr.NewErrorWithCode(
		err,
		message,
		cPackage,
		function,
		http.StatusBadRequest,
		errCode,
	)
}

// errCommonValidation - bad request error without error
func errCommonValidation(function, message, errCode string) gobol.Error {
	return NewValidationError(function, message, fmt.Errorf(message), errCode)
}

var (
	ErrParsingMetric       = errCommonValidation("ParsePoint", `Error parsing "metric" from JSON.`, "C01")
	ErrParsingTimestamp    = errCommonValidation("ParsePoint", `Error parsing "timestamp" from JSON.`, "C02")
	ErrParsingValue        = errCommonValidation("ParsePoint", `Error parsing "value" from JSON.`, "C03")
	ErrParsingText         = errCommonValidation("ParsePoint", `Error parsing "text" from JSON.`, "C04")
	ErrParsingTagKey       = errCommonValidation("ParsePoint", `Error parsing tag key from JSON.`, "C05")
	ErrParsingTagValue     = errCommonValidation("ParsePoint", `Error parsing tag value from JSON.`, "C06")
	ErrNumberTypeExpected  = errCommonValidation("ValidateType", `Wrong Format: Field "value" is required.`, "C07")
	ErrTextTypeExpected    = errCommonValidation("ValidateType", `Wrong Format: Field "text" is required.`, "C08")
	ErrMaxTextValueSize    = errCommonValidation("ValidateType", `Wrong Format: Field "text" has exceeded the maximum number of characters allowed`, "C09")
	ErrNoTags              = errCommonValidation("ValidateTags", `Wrong Format: At least one tag is required.`, "C10")
	ErrDuplicatedTags      = errCommonValidation("ValidateTags", `Wrong Format: Duplicated tags.`, "C11")
	ErrNoUserTags          = errCommonValidation("ValidateTags", `Wrong Format: At least one tag other than "ksid" and "ttl" is required.`, "C12")
	ErrNoKeysetTag         = errCommonValidation("ValidateKeyset", `Wrong Format: Tag "ksid" is required.`, "C13")
	ErrInvalidKeysetFormat = errCommonValidation("ValidateKeyset", `Wrong Format: Field "ksid" has a invalid format.`, "C14")
	ErrInvalidTagKey       = errCommonValidation("ValidateProperty", `Wrong Format: Tag key has a invalid format.`, "C15")
	ErrInvalidTagValue     = errCommonValidation("ValidateProperty", `Wrong Format: Tag value has a invalid format.`, "C16")
	ErrInvalidMetric       = errCommonValidation("ValidateProperty", `Wrong Format: Field "metric" (%s) is not well formed.`, "C17")
	ErrInvalidPropertyType = errCommonValidation("ValidateProperty", `Property type is not mapped`, "C18")
	ErrInvalidTTLValue     = errCommonValidation("ParseTTL", `Wrong Format: Tag "ttl" must be a positive number.`, "C19")
	ErrInexistentKeyset    = errCommonValidation("ValidateKeyset", `Keyset not exists.`, "C20")
	ErrMalformedJSON       = errCommonValidation("ParsePoint", `JSON is malformed.`, "C21")
	ErrInvalidTimestamp    = errCommonValidation("ValidateTimestamp", `Wrong Format: timestamp has a invalid format.`, "C22")
	ErrReadingJSONBytes    = errCommonValidation("ParsePointArray", "Error reading JSON bytes.", "C23")
)
