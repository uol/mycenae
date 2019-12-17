package parser

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

const (
	cPackage string = "parser"
)

func errBasic(function, msg string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			msg,
			cPackage,
			function,
			http.StatusBadRequest,
		)
	}
	return nil
}

func errParams(function, msg string, e error) gobol.Error {
	return errBasic(function, msg, e)
}

func errDoubleFunc(function, msg string) gobol.Error {
	s := fmt.Sprintf("You can use only one %s function per expression", msg)
	return errBasic(function, s, errors.New(s))
}

func errGroup(msg string) gobol.Error {
	return errBasic("parseGroup", msg, errors.New(msg))
}

func errBadUnit() gobol.Error {
	s := "Invalid unit"
	return errBasic("GetRelativeStart", s, errors.New(s))
}

func errGRT(e error) gobol.Error {
	var es string
	if e != nil {
		es = e.Error()
	}
	return errBasic("GetRelativeStart", es, e)
}

func errParseMap(msg string) gobol.Error {
	return errBasic("parseMap", msg, errors.New(msg))
}

func errRateCounter(e error) gobol.Error {
	return errBasic("parseRate", "rate counter, the 1st parameter, needs to be a boolean", e)
}

func errRateCounterMax(e error) gobol.Error {
	return errBasic("parseRate", `rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'`, e)
}

func errRateResetValue(e error) gobol.Error {
	return errBasic("parseRate", "rate resetValue, the 3rd parameter, needs to be an integer", e)
}

func errUnkFunc(msg string) gobol.Error {
	return errBasic("parseExpression", msg, errors.New(msg))
}
