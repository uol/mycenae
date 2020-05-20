package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/cors"
	"github.com/uol/logh"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/telnetmgr"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/config"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/structs"
	tlmanager "github.com/uol/timeline-manager"
)

// New returns http handler to the endpoints
func New(
	timelineManager *tlmanager.TimelineManager,
	p *plot.Plot,
	keyspace *keyspace.Keyspace,
	mc *memcached.Memcached,
	collector *collector.Collector,
	set structs.SettingsHTTP,
	ks *keyset.Manager,
	telnetManager *telnetmgr.Manager,
) *REST {

	return &REST{
		probeStatus:     http.StatusOK,
		logger:          logh.CreateContextualLogger(constants.StringsPKG, "rest"),
		timelineManager: timelineManager,
		reader:          p,
		kspace:          keyspace,
		memcached:       mc,
		writer:          collector,
		settings:        set,
		keyset:          ks,
		telnetManager:   telnetManager,
	}
}

// REST is the http handler
type REST struct {
	probeStatus int

	logger          *logh.ContextualLogger
	timelineManager *tlmanager.TimelineManager
	reader          *plot.Plot
	kspace          *keyspace.Keyspace
	memcached       *memcached.Memcached
	writer          *collector.Collector
	settings        structs.SettingsHTTP
	server          *http.Server
	keyset          *keyset.Manager
	telnetManager   *telnetmgr.Manager
}

// Start asynchronously the handler of the APIs
func (trest *REST) Start() {

	go trest.asyncStart()

}

func (trest *REST) asyncStart() {

	rip.SetLogger(trest.settings.ForceErrorAsDebug)

	router := rip.NewCustomRouter()
	//NODE TO NODE
	router.HEAD("/node/connections", trest.telnetManager.CountConnections)
	router.HEAD("/node/halt/balancing", trest.telnetManager.HaltTelnetBalancingProcess)
	//PROBE
	router.GET("/probe", trest.check)
	//EXPRESSION
	router.GET("/expression/check", trest.reader.ExpressionCheckGET)
	router.POST("/expression/check", trest.reader.ExpressionCheckPOST)
	router.POST("/expression/compile", trest.reader.ExpressionCompile)
	router.GET("/expression/parse", trest.reader.ExpressionParseGET)
	router.POST("/expression/parse", trest.reader.ExpressionParsePOST)
	router.GET("/keysets/:keyset/expression/expand", trest.reader.ExpressionExpandGET)
	router.POST("/keysets/:keyset/expression/expand", trest.reader.ExpressionExpandPOST)
	//NUMBER
	router.GET("/keysets/:keyset/tags", trest.reader.ListTagsNumber)
	router.GET("/keysets/:keyset/metrics", trest.reader.ListMetricsNumber)
	router.POST("/keysets/:keyset/meta", trest.reader.ListMetaNumber)
	router.GET("/keysets/:keyset/values", trest.reader.ListMetaNumber)
	router.GET("/keysets/:keyset/metric/tag/keys", trest.reader.ListNumberTagKeysByMetric)
	router.GET("/keysets/:keyset/metric/tag/values", trest.reader.ListNumberTagValuesByMetric)
	//TEXT
	router.GET("/keysets/:keyset/text/tags", trest.reader.ListTagsText)
	router.GET("/keysets/:keyset/text/metrics", trest.reader.ListMetricsText)
	router.POST("/keysets/:keyset/text/meta", trest.reader.ListMetaText)
	router.GET("/keysets/:keyset/text/tag/keys", trest.reader.ListTextTagKeysByMetric)
	router.GET("/keysets/:keyset/text/tag/values", trest.reader.ListTextTagValuesByMetric)
	//KEYSPACE
	router.GET("/datacenters", trest.kspace.ListDC)
	router.HEAD("/keyspaces/:keyspace", trest.kspace.Check)
	router.POST("/keyspaces/:keyspace", trest.kspace.Create)
	router.PUT("/keyspaces/:keyspace", trest.kspace.Update)
	router.GET("/keyspaces", trest.kspace.GetAll)
	//WRITE
	router.POST("/api/put", trest.writer.HandleNumber)
	router.PUT("/api/put", trest.writer.HandleNumber)
	router.POST("/api/text/put", trest.writer.HandleText)
	//OPENTSDB
	router.POST("/keysets/:keyset/api/query", trest.reader.Query)
	router.GET("/keysets/:keyset/api/suggest", trest.reader.Suggest)
	router.GET("/keysets/:keyset/api/search/lookup", trest.reader.Lookup)
	router.GET("/keysets/:keyset/api/aggregators", config.Aggregators)
	router.GET("/keysets/:keyset/api/config/filters", config.Filters)
	//HYBRIDS
	router.POST("/keysets/:keyset/query/expression", trest.reader.ExpressionQueryPOST)
	router.GET("/keysets/:keyset/query/expression", trest.reader.ExpressionQueryGET)
	//RAW POINTS API
	router.POST("/api/query/raw", trest.reader.RawDataQuery)
	//KEYSETS
	router.POST("/keysets/:keyset", trest.keyset.CreateKeyset)
	router.HEAD("/keysets/:keyset", trest.keyset.Check)
	router.DELETE("/keysets/:keyset", trest.keyset.DeleteKeyset)
	router.GET("/keysets", trest.keyset.GetKeysets)
	//DELETE
	router.POST("/keysets/:keyset/delete/meta", trest.reader.DeleteNumberTS)
	router.POST("/keysets/:keyset/delete/text/meta", trest.reader.DeleteTextTS)
	//DEPRECATED
	router.POST("/keysets/:keyset/points", trest.reader.ListPoints)
	//ADMINISTRATIVE
	router.POST("/admin/free-os-memory", trest.freeOSMemory)
	router.POST("/admin/set-gc-percent", trest.setGCPercent)
	router.GET("/admin/read-gc-stats", trest.readGCStats)

	if trest.settings.EnableProfiling {

		if logh.WarnEnabled {
			trest.logger.Warn().Msg("WARNING - http profiling is enabled!!!")
		}

		router.Handler(http.MethodGet, "/debug/pprof/:item", http.DefaultServeMux)
	}

	var compositeHTTPHandlers http.Handler

	logHandler := rip.NewLogMiddleware(router, trest.settings.Port, newRestStatistics(trest.timelineManager))

	if trest.settings.AllowCORS {
		compositeHTTPHandlers = cors.AllowAll().Handler(logHandler)
	} else {
		compositeHTTPHandlers = logHandler
	}

	trest.server = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", trest.settings.Bind, trest.settings.Port),
		Handler:           compositeHTTPHandlers,
		ReadTimeout:       60 * time.Second,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      60 * time.Second,
		MaxHeaderBytes:    10485760,
	}

	err := trest.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		if logh.ErrorEnabled {
			trest.logger.Error().Err(err).Send()
		}
	}
}

func (trest *REST) check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	w.WriteHeader(http.StatusOK)
}

// Stop - stops the rest server
func (trest *REST) Stop() {
	if err := trest.server.Shutdown(context.Background()); err != nil {
		if logh.ErrorEnabled {
			trest.logger.Error().Err(err).Send()
		}
	}
}
