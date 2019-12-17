package metadata

import (
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tserr"
)

const cPackage string = "metadata"

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

func errConflict(function string, err error) gobol.Error {
	return errBasic(function, constants.StringsEmpty, http.StatusConflict, err)
}

func errInternalServer(function string, err error) gobol.Error {
	return errBasic(function, constants.StringsEmpty, http.StatusInternalServerError, err)
}
