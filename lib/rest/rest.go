package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"go.uber.org/zap/zapcore"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/gobol/snitch"
	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/config"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/structs"
)

// New returns http handler to the endpoints
func New(
	log *structs.TsLog,
	gbs *snitch.Stats,
	p *plot.Plot,
	keyspace *keyspace.Keyspace,
	mc *memcached.Memcached,
	collector *collector.Collector,
	set structs.SettingsHTTP,
	probeThreshold float64,
	ks *keyset.KeySet,
) *REST {

	return &REST{
		probeThreshold: probeThreshold,
		probeStatus:    http.StatusOK,
		closed:         make(chan struct{}),

		gblog:     log.General,
		sts:       gbs,
		reader:    p,
		kspace:    keyspace,
		memcached: mc,
		writer:    collector,
		settings:  set,
		keyset:    ks,
	}
}

// REST is the http handler
type REST struct {
	probeThreshold float64
	probeStatus    int
	closed         chan struct{}

	gblog     *zap.Logger
	sts       *snitch.Stats
	reader    *plot.Plot
	kspace    *keyspace.Keyspace
	memcached *memcached.Memcached
	writer    *collector.Collector
	settings  structs.SettingsHTTP
	server    *http.Server
	keyset    *keyset.KeySet
}

// Start asynchronously the handler of the APIs
func (trest *REST) Start() {

	go trest.asyncStart()

}

func (trest *REST) asyncStart() {

	lf := []zapcore.Field{
		zap.String("package", "rest"),
		zap.String("func", "asyncStart"),
	}

	rip.SetLogger(trest.gblog)

	pathMatcher := regexp.MustCompile(`^(/[a-zA-Z0-9._-]+)?/$`)

	if !pathMatcher.Match([]byte(trest.settings.Path)) {
		err := errors.New("Invalid path to start rest service")

		if err != nil {
			trest.gblog.Fatal(fmt.Sprintf("ERROR - Starting REST: %s", err.Error()), lf...)
		}
	}

	path := trest.settings.Path

	router := rip.NewCustomRouter()
	//PROBE
	router.GET(path+"probe", trest.check)
	//READ
	router.POST(path+"keysets/:keyset/points", trest.reader.ListPoints)
	//EXPRESSION
	router.GET(path+"expression/check", trest.reader.ExpressionCheckGET)
	router.POST(path+"expression/check", trest.reader.ExpressionCheckPOST)
	router.POST(path+"expression/compile", trest.reader.ExpressionCompile)
	router.GET(path+"expression/parse", trest.reader.ExpressionParseGET)
	router.POST(path+"expression/parse", trest.reader.ExpressionParsePOST)
	router.GET(path+"keysets/:keyset/expression/expand", trest.reader.ExpressionExpandGET)
	router.POST(path+"keysets/:keyset/expression/expand", trest.reader.ExpressionExpandPOST)
	//NUMBER
	router.GET(path+"keysets/:keyset/tags", trest.reader.ListTagsNumber)
	router.GET(path+"keysets/:keyset/metrics", trest.reader.ListMetricsNumber)
	router.POST(path+"keysets/:keyset/meta", trest.reader.ListMetaNumber)
	router.GET(path+"keysets/:keyset/values", trest.reader.ListMetaNumber)
	router.GET(path+"keysets/:keyset/metric/tag/keys", trest.reader.ListNumberTagKeysByMetric)
	router.GET(path+"keysets/:keyset/metric/tag/values", trest.reader.ListNumberTagValuesByMetric)
	//TEXT
	router.GET(path+"keysets/:keyset/text/tags", trest.reader.ListTagsText)
	router.GET(path+"keysets/:keyset/text/metrics", trest.reader.ListMetricsText)
	router.POST(path+"keysets/:keyset/text/meta", trest.reader.ListMetaText)
	router.GET(path+"keysets/:keyset/text/tag/keys", trest.reader.ListTextTagKeysByMetric)
	router.GET(path+"keysets/:keyset/text/tag/values", trest.reader.ListTextTagValuesByMetric)
	//KEYSPACE
	router.GET(path+"datacenters", trest.kspace.ListDC)
	router.HEAD(path+"keyspaces/:keyspace", trest.kspace.Check)
	router.POST(path+"keyspaces/:keyspace", trest.kspace.Create)
	router.PUT(path+"keyspaces/:keyspace", trest.kspace.Update)
	router.GET(path+"keyspaces", trest.kspace.GetAll)
	//WRITE
	router.POST(path+"api/put", trest.writer.HandleNumber)
	router.PUT(path+"api/put", trest.writer.HandleNumber)
	router.POST(path+"api/text/put", trest.writer.HandleText)
	//OPENTSDB
	router.POST("/keysets/:keyset/api/query", trest.reader.Query)
	router.GET("/keysets/:keyset/api/suggest", trest.reader.Suggest)
	router.GET("/keysets/:keyset/api/search/lookup", trest.reader.Lookup)
	router.GET("/keysets/:keyset/api/aggregators", config.Aggregators)
	router.GET("/keysets/:keyset/api/config/filters", config.Filters)
	//HYBRIDS
	router.POST("/keysets/:keyset/query/expression", trest.reader.ExpressionQueryPOST)
	router.GET("/keysets/:keyset/query/expression", trest.reader.ExpressionQueryGET)
	//KEYSETS
	router.POST("/keysets/:keyset", trest.keyset.CreateKeySet)
	router.HEAD("/keysets/:keyset", trest.keyset.Check)
	router.GET("/keysets", trest.keyset.GetKeySets)
	//DELETE
	router.POST(path+"keysets/:keyset/delete/meta", trest.reader.DeleteNumberTS)
	router.POST(path+"keysets/:keyset/delete/text/meta", trest.reader.DeleteTextTS)

	trest.server = &http.Server{
		Addr: fmt.Sprintf("%s:%s", trest.settings.Bind, trest.settings.Port),
		Handler: rip.NewLogMiddleware(
			"mycenae",
			"mycenae",
			trest.gblog,
			trest.sts,
			rip.NewGzipMiddleware(rip.BestSpeed, router),
			true,
		),
	}

	err := trest.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		trest.gblog.Error(err.Error(), lf...)
	}
	trest.closed <- struct{}{}
}

func (trest *REST) check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	ratio := trest.writer.ReceivedErrorRatio()

	if ratio < trest.probeThreshold {
		w.WriteHeader(trest.probeStatus)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (trest *REST) Stop() {

	lf := []zapcore.Field{
		zap.String("package", "rest"),
		zap.String("func", "Stop"),
	}

	trest.probeStatus = http.StatusServiceUnavailable

	if err := trest.server.Shutdown(context.Background()); err != nil {
		trest.gblog.Error(err.Error(), lf...)
	}

	<-trest.closed
}
