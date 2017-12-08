package metadata

import (
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(
	method, structure, message string, code int, err error,
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

func newPersistenceError(method, structure string, err error) gobol.Error {
	return errBasic(
		method,
		structure,
		err.Error(),
		http.StatusInternalServerError,
		err,
	)
}
