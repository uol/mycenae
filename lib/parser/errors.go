package parser

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusBadRequest,
			[]zapcore.Field{
				zap.String("package", "parse"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errParams(f, m string, e error) gobol.Error {
	return errBasic(f, m, e)
}

func errDoubleFunc(f, ef string) gobol.Error {
	s := fmt.Sprintf("You can use only one %s function per expression", ef)
	return errBasic(f, s, errors.New(s))
}

func errGroup(s string) gobol.Error {
	return errBasic("parseGroup", s, errors.New(s))
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

func errParseMap(s string) gobol.Error {
	return errBasic("parseMap", s, errors.New(s))
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

func errUnkFunc(s string) gobol.Error {
	return errBasic("parseExpression", s, errors.New(s))
}
