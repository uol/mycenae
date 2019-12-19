package config

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
)

func Aggregators(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if keyset := ps.ByName(constants.StringsKeyset); keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/aggregators", constants.StringsKeyset: "empty"})
	} else {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/aggregators", constants.StringsKeyset: keyset})
	}
	rip.SuccessJSON(w, http.StatusOK, GetAggregators())
}

func Filters(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if keyset := ps.ByName(constants.StringsKeyset); keyset == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/config/filters", constants.StringsKeyset: "empty"})
	} else {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset/api/config/filters", constants.StringsKeyset: keyset})
	}
	rip.SuccessJSON(w, http.StatusOK, GetFiltersFull())
}
