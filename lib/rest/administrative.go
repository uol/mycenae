package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"runtime/debug"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/logh"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
)

func (trest *REST) freeOSMemory(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "freeOSMemory").Msg("calling")
	}

	debug.FreeOSMemory()

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "freeOSMemory").Msg("done")
	}

	w.WriteHeader(http.StatusOK)
}

func (trest *REST) setGCPercent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	q := r.URL.Query()
	percentageStr := q.Get("percentage")
	if len(percentageStr) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	percentage, err := strconv.Atoi(percentageStr)
	if err != nil {
		if logh.ErrorEnabled {
			trest.logger.Error().Err(err).Str(constants.StringsFunc, "setGCPercent").Msg("calling")
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "setGCPercent").Msg("calling")
	}

	old := debug.SetGCPercent(percentage)

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "setGCPercent").Msg("done")
	}

	rip.Success(w, http.StatusOK, ([]byte(strconv.Itoa(old))))
}

func (trest *REST) readGCStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "readGCStats").Msg("calling")
	}

	gcstats := debug.GCStats{}
	debug.ReadGCStats(&gcstats)

	if logh.InfoEnabled {
		trest.logger.Info().Str(constants.StringsFunc, "readGCStats").Msg("done")
	}

	prettyBytes, err := json.MarshalIndent(gcstats, constants.StringsEmpty, "  ")
	if err != nil {
		if logh.ErrorEnabled {
			trest.logger.Error().Err(err).Str(constants.StringsFunc, "readGCStats").Msg("calling")
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	rip.Success(w, http.StatusOK, prettyBytes)
}
