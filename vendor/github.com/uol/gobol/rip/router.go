package rip

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/loader"
)

func NewCustomRouter() *httprouter.Router {

	router := httprouter.New()
	router.MethodNotAllowed = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		})
	router.NotFound = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
	return router

}

var mapErrorMessage map[string]string = make(map[string]string)

// NewCustomRouterMapError returns a httprouter.Router and maps error code to error messages according to errorMessagesFile
func NewCustomRouterMapError(errorMessagesFile string) *httprouter.Router {
	err := loader.ConfJson(errorMessagesFile, &mapErrorMessage)
	if err != nil {
		fmt.Println(fmt.Sprintf("error loading config file %s: %s", errorMessagesFile, err.Error()))
	}

	return NewCustomRouter()

}
