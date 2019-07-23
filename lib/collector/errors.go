package collector

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errBadRequest(function, message string, err error) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			message,
			http.StatusBadRequest,
			[]zapcore.Field{
				zap.String("package", "collector"),
				zap.String("func", function),
			},
		)
	}
	return nil
}

func errInternalServerError(function, message string, err error) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			message,
			http.StatusInternalServerError,
			[]zapcore.Field{
				zap.String("package", "collector"),
				zap.String("func", function),
			},
		)
	}
	return nil
}

func errValidation(s string) gobol.Error {
	return errBadRequest("makePacket", s, errors.New(s))
}

func errUnmarshal(f string, e error) gobol.Error {
	return errBadRequest(f, "Wrong JSON format", e)
}

func errPersist(f string, e error) gobol.Error {
	return errInternalServerError(f, e.Error(), e)
}
