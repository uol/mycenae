package collector

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

const (
	cMakePacket  = "makePacket"
	cWrongFormat = "Wrong JSON format"
	cPackage     = "collector"
)

func errBadRequest(function, message string, err error) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			cPackage,
			function,
			message,
			http.StatusBadRequest,
		)
	}
	return nil
}

func errInternalServerError(function, message string, err error) gobol.Error {
	if err != nil {
		return tserr.New(
			err,
			cPackage,
			function,
			message,
			http.StatusInternalServerError,
		)
	}
	return nil
}

func errValidation(msg string) gobol.Error {
	return errBadRequest(cMakePacket, msg, errors.New(msg))
}

func errUnmarshal(function string, e error) gobol.Error {
	return errBadRequest(function, cWrongFormat, e)
}

func errPersist(function string, e error) gobol.Error {
	return errInternalServerError(function, e.Error(), e)
}

func errMultipleErrors(function string, gerrs []gobol.Error) gobol.Error {
	return gerrs[0]
}
