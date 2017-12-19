package persistence

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

func newUnimplementedMethod(funcname, structure string) gobol.Error {
	const message = "Not implemented method"
	fields := map[string]interface{}{
		"function":  funcname,
		"structure": structure,
		"package":   "persistence",
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
			map[string]interface{}{
				"package":   "keyspace",
				"structure": structure,
				"method":    method,
			},
		)
	}
	return nil
}

func errNoContent(method, structure string) gobol.Error {
	return errBasic(method, structure, "", http.StatusNoContent, errors.New(""))
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
