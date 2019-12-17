package keyspace

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tserr"
)

const (
	cPackage string = "keyspace"
)

func errBasic(function, msg string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			msg,
			cPackage,
			function,
			code,
		)
	}
	return nil
}

func errValidationS(function, msg string) gobol.Error {
	return errBasic(function, msg, http.StatusBadRequest, errors.New(msg))
}

func errNotFound(function string) gobol.Error {
	return errBasic(function, constants.StringsEmpty, http.StatusNotFound, errors.New(constants.StringsEmpty))
}

func errNoContent(function string) gobol.Error {
	return errBasic(function, constants.StringsEmpty, http.StatusNoContent, errors.New(constants.StringsEmpty))
}
