package rip

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/rs/cors"
	"github.com/uol/logh"
	"github.com/uol/gobol/snitch"
)

type key int

const (
	statsTagskey key = 0
)

type LogResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (w *LogResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	size, err := w.ResponseWriter.Write(b)
	w.size += size
	return size, err
}

func (w *LogResponseWriter) WriteHeader(s int) {
	w.ResponseWriter.WriteHeader(s)
	w.status = s
}

func (w *LogResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func NewLogMiddleware(service, system string, sts *snitch.Stats, next http.Handler, allowCORS bool) *LogHandler {
	var fullHandler http.Handler
	if allowCORS {
		fullHandler = cors.AllowAll().Handler(next)
	} else {
		fullHandler = next
	}
	return &LogHandler{
		service:   service,
		system:    system,
		next:      fullHandler,
		stats:     sts,
		allowCORS: allowCORS,
		connectionStatsTags: map[string]string{
			"type":   "tcp",
			"source": "http",
		},
	}
}

type LogHandler struct {
	service             string
	system              string
	next                http.Handler
	stats               *snitch.Stats
	allowCORS           bool
	connectionStatsTags map[string]string
}

func (h *LogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	go h.incrementHTTPConn()

	start := time.Now()

	rid := uuid.NewRandom().String()

	header := w.Header()

	header.Add(fmt.Sprintf("X-REQUEST-%s-ID", h.service), rid)

	if header.Get(fmt.Sprintf("X-REQUEST-%s-ID", h.system)) == "" {
		header.Add(fmt.Sprintf("X-REQUEST-%s-ID", h.system), rid)
	}

	logw := &LogResponseWriter{
		ResponseWriter: w,
	}

	ctx := context.Background()

	userTags := sync.Map{}

	h.next.ServeHTTP(logw, r.WithContext(context.WithValue(ctx, statsTagskey, &userTags)))

	status := logw.status

	d := time.Since(start)

	tags := map[string]string{
		"protocol": r.Proto,
		"method":   r.Method,
		"status":   strconv.Itoa(status),
	}

	if status != 404 && status != 405 {
		tags["path"] = r.URL.Path
	}

	userTags.Range(func(k, v interface{}) bool {
		tags[k.(string)] = v.(string)
		return true
	})

	h.increment("request.count", tags)
	h.valueAdd("request.duration", tags, float64(d.Nanoseconds())/float64(time.Millisecond))
}

func AddStatsMap(r *http.Request, tags map[string]string) {
	userTags, ok := r.Context().Value(statsTagskey).(*sync.Map)
	if ok {
		for k, v := range tags {
			userTags.Store(k, v)
		}
	}
}

func (h *LogHandler) increment(metric string, tags map[string]string) {
	err := h.stats.Increment(metric, tags, "@every 1m", false, true)
	if err != nil {
		if ev := logError(customError{msg: err.Error(), pkg: "log_middleware", function: "increment"}); ev != nil {
			ev.Str("metric", metric)
		}
	}
}

func (h *LogHandler) valueAdd(metric string, tags map[string]string, v float64) {
	err := h.stats.ValueAdd(metric, tags, "avg", "@every 1m", false, false, v)
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Str("metric", metric).Str("pkg", "log_middleware").Str("func", "valueAdd").Err(err).Send()
		}
	}
}

func (h *LogHandler) incrementHTTPConn() {
	err := h.stats.Increment("network.connection", h.connectionStatsTags, "@every 10s", false, true)
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Str("pkg", "log_middleware").Str("func", "incrementHTTPConn").Err(err).Send()
		}
	}
}
