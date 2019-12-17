package keyset

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tserr"
)

const (
	cPackage string = "keyset"
)

func errBasic(function, message string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			message,
			cPackage,
			function,
			code,
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

func errNotFound(function string) gobol.Error {
	return errBasic(function, constants.StringsEmpty, http.StatusNotFound, errors.New(constants.StringsEmpty))
}
