package telnet

import (
	"errors"

	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/telnetsrv"
	"github.com/uol/mycenae/lib/tserr"
)

//
// Add custom validation errors for telnet.
// author: rnojiri
//

const (
	cPackage                string = "telnet"
	cFuncHandle             string = "Handle"
	cFuncParse              string = "Parse"
	cMsgFInvalidTimestamp   string = "invalid timestamp: %s"
	cMsgFInvalidTTL         string = "invalid ttl: %s"
	cMsgFInvalidMetric      string = "invalid metric: %s"
	cMsgFInvalidKSID        string = "invalid ksid: %s"
	cMsgFInvalidKey         string = "invalid key: %s"
	cMsgFInvalidValue       string = "invalid value: %s"
	cMsgFKSIDTagNotFound    string = "no ksid tag found: %s"
	cMsgFInvalidTags        string = "tags validation failure: %s"
	cMsgFPointCreationError string = "point creation error: %s"
	cMsgFInvalidLineContent string = "error reading line content: %s"
	cMsgEmptyLine           string = "empty line received"
)

// newValidationError - telnet error
func newValidationError(function, message string, errCode string) gobol.Error {
	return tserr.NewErrorWithCode(
		errors.New(message),
		message,
		cPackage,
		function,
		0,
		errCode,
	)
}

func logAndStats(handler interface{}, gerr gobol.Error, funcName, keyset, ip, message string, parameters ...interface{}) {

	h := handler.(telnetsrv.TelnetDataHandler)

	h.GetValidationService().StatsValidationError(funcName, keyset, ip, h.GetSourceType(), gerr)

	if !h.SilenceLogs() && logh.ErrorEnabled {
		ev := h.GetLogger().Error().Err(gerr)

		if len(funcName) > 0 {
			ev = ev.Str(constants.StringsFunc, funcName)
		}

		if len(parameters) > 0 {
			ev.Msgf(message, parameters...)
		} else {
			ev.Msg(message)
		}
	}
}

var (
	errDataFormatParse      = newValidationError(cFuncParse, "error parsing the input data", "T01")
	errSpecialCommandFormat = newValidationError(cFuncHandle, "error in the special command format", "T02")
)
