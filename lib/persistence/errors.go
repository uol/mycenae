package persistence

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newUnimplementedMethod(funcname, structure string) gobol.Error {
	const message = "Not implemented method"
	fields := []zapcore.Field{
		zap.String("package", "persistence"),
		zap.String("structure", structure),
		zap.String("func", funcname),
	}
	return tserr.New(
		fmt.Errorf(message), message, http.StatusInternalServerError, fields,
	)
}

func errBasic(
	method, structure, message string,
	code int, err error,
) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			message,
			code,
			[]zapcore.Field{
				zap.String("package", "keyspace"),
				zap.String("structure", structure),
				zap.String("func", method),
			},
		)
	}
	return nil
}

func errNoContent(method, structure string) gobol.Error {
	return errBasic(method, structure, "", http.StatusNoContent, errors.New(""))
}

func errNoDatacenter(method, structure, message string) gobol.Error {
	return errBasic(
		method, structure, message,
		http.StatusBadRequest, errors.New(message),
	)
}

func errNotFound(method, structure, message string) gobol.Error {
	return errBasic(
		method, structure, message,
		http.StatusNotFound, errors.New(message),
	)
}

func errPersist(method, structure string, err error) gobol.Error {
	return errBasic(method, structure, err.Error(), http.StatusInternalServerError, err)
}

func errConflict(method, structure, message string) gobol.Error {
	return errBasic(
		method, structure, message,
		http.StatusConflict, errors.New(message),
	)
}
