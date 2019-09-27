package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"runtime/debug"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (trest *REST) freeOSMemory(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	lf := []zapcore.Field{
		zap.String("action", "ADMINISTRATIVE"),
		zap.String("package", "rest"),
		zap.String("func", "freeOSMemory"),
	}

	trest.gblog.Info("calling", lf...)

	debug.FreeOSMemory()

	trest.gblog.Info("done", lf...)

	w.WriteHeader(http.StatusOK)
}

func (trest *REST) setGCPercent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	lf := []zapcore.Field{
		zap.String("action", "ADMINISTRATIVE"),
		zap.String("package", "rest"),
		zap.String("func", "setGCPercent"),
	}

	q := r.URL.Query()
	percentageStr := q.Get("percentage")
	if len(percentageStr) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	percentage, err := strconv.Atoi(percentageStr)
	if err != nil {
		trest.gblog.Error(err.Error(), lf...)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	trest.gblog.Info("calling", lf...)

	old := debug.SetGCPercent(percentage)

	trest.gblog.Info("done", lf...)

	rip.Success(w, http.StatusOK, ([]byte(strconv.Itoa(old))))
}

func (trest *REST) readGCStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	lf := []zapcore.Field{
		zap.String("action", "ADMINISTRATIVE"),
		zap.String("package", "rest"),
		zap.String("func", "readGCStats"),
	}

	trest.gblog.Info("calling", lf...)

	gcstats := debug.GCStats{}
	debug.ReadGCStats(&gcstats)

	trest.gblog.Info("done", lf...)

	prettyBytes, err := json.MarshalIndent(gcstats, "", "  ")
	if err != nil {
		trest.gblog.Error(err.Error(), lf...)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	rip.Success(w, http.StatusOK, prettyBytes)
}
