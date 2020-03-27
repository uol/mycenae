package config

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func Aggregators(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	rip.SuccessJSON(w, http.StatusOK, GetAggregators())
}

func Filters(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	rip.SuccessJSON(w, http.StatusOK, GetFiltersFull())
}
