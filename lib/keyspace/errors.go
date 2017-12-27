package keyspace

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			code,
			map[string]interface{}{
				"package": "keyspace",
				"func":    f,
			},
		)
	}
	return nil
}

func errValidationS(f, s string) gobol.Error {
	return errBasic(f, s, http.StatusBadRequest, errors.New(s))
}

func errNotFound(f string) gobol.Error {
	return errBasic(f, "", http.StatusNotFound, errors.New(""))
}

func errNoContent(f string) gobol.Error {
	return errBasic(f, "", http.StatusNoContent, errors.New(""))
}
