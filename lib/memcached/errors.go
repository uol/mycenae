package memcached

import (
	"net/http"

	"github.com/uol/gobol"

	"errors"
	"github.com/uol/mycenae/lib/tserr"
)

func errInternalServerErrorM(function, message string) gobol.Error {

	return errInternalServerError(function, message, errors.New(message))
}

func errInternalServerError(function, message string, e error) gobol.Error {

	return tserr.New(
		e,
		message,
		http.StatusInternalServerError,
		map[string]interface{}{
			"package": "memcached/persistence",
			"func":    function,
		},
	)
}
