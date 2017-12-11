package persistence

import (
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

func newUnimplementedMethod(funcname, structure string) gobol.Error {
	const message = "Not implemeted leave"
	fields := map[string]interface{}{
		"function":  funcname,
		"structure": structure,
		"package":   "persistence",
	}
	return tserr.New(
		fmt.Errorf(message), message, http.StatusInternalServerError, fields,
	)
}
