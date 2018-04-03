package rip

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
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
