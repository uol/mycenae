package persistence

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tserr"
)

const (
	cPackage string = "persistence"
	cMessage string = "Not implemented method"
)

func newUnimplementedMethod(funcname, structure string) gobol.Error {
	return tserr.New(
		fmt.Errorf(cMessage),
		cMessage,
		cPackage,
		funcname,
		http.StatusInternalServerError,
	)
}

func errBasic(
	function, structure, message string,
	code int, err error,
) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			message,
			cPackage,
			function,
			code,
		)
	}
	return nil
}

func errNoContent(method, structure string) gobol.Error {
	return errBasic(method, structure, constants.StringsEmpty, http.StatusNoContent, errors.New(constants.StringsEmpty))
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
