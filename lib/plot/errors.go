package plot

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errInit(s string) gobol.Error {
	return tserr.New(
		errors.New(s),
		s,
		http.StatusInternalServerError,
		[]zapcore.Field{
			zap.String("package", "plot"),
			zap.String("func", "New"),
		},
	)
}

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			code,
			[]zapcore.Field{
				zap.String("package", "plot"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errValidationS(f, s string) gobol.Error {
	return errBasic(f, s, http.StatusBadRequest, errors.New(s))
}

func errNotFound(f string) gobol.Error {
	return errBasic(f, "", http.StatusNotFound, errors.New(""))
}

func errValidation(f, m string, e error) gobol.Error {
	return errBasic(f, m, http.StatusBadRequest, e)
}

func errNoContent(f string) gobol.Error {
	return errBasic(f, "", http.StatusNoContent, errors.New(""))
}

func errParamSize(f string, e error) gobol.Error {
	return errBasic(f, `query param "size" should be an integer number greater than zero`, http.StatusBadRequest, e)
}

func errParamFrom(f string, e error) gobol.Error {
	return errBasic(f, `query param "from" should be an integer number greater or equals zero`, http.StatusBadRequest, e)
}

func errPersist(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusInternalServerError, e)
}

func errValidationE(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusBadRequest, e)
}

func errEmptyExpression(f string) gobol.Error {
	return errBasic(f, "no expression found", http.StatusBadRequest, errors.New("no expression found"))
}

func errMandatoryParam(function string, parameter string) gobol.Error {
	return errBasic(function, "query string parameter \""+parameter+"\" is mandatory", http.StatusBadRequest, errors.New(""))
}

func errMaxBytesLimitWrapper(function string, err error) gobol.Error {
	return errBasic(function, err.Error(), 413, err)
}

func errMaxBytesLimit(function, keyset, metric string, start, end int64, ttl int) gobol.Error {
	return errBasic(function, "payload too large", 413, fmt.Errorf("max bytes reached: keyset '%s', metric '%s', start '%d', end '%d', ttl '%d'", keyset, metric, start, end, ttl))
}
