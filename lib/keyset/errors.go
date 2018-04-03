package keyset

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(function, message string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			message,
			code,
			map[string]interface{}{
				"package": "keyset",
				"func":    function,
			},
		)
	}
	return nil
}

func errBadRequest(function, message string) gobol.Error {
	return errBasic(function, message, http.StatusBadRequest, errors.New(message))
}

func errInternalServerError(function string, e error) gobol.Error {
	return errBasic(function, e.Error(), http.StatusInternalServerError, e)
}

func errNotFound(f string) gobol.Error {
	return errBasic(f, "", http.StatusNotFound, errors.New(""))
}
