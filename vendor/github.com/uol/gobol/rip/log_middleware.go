package rip

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"github.com/rs/cors"
	"github.com/uol/gobol/snitch"
	"go.uber.org/zap"
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

func NewLogMiddleware(service, system string, logger *zap.Logger, sts *snitch.Stats, next http.Handler, allowCORS bool) *LogHandler {
	logger = logger.WithOptions(zap.AddStacktrace(zap.PanicLevel))
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
		logger:    logger,
		stats:     sts,
		allowCORS: allowCORS,
	}
}

type LogHandler struct {
	service   string
	system    string
	next      http.Handler
	logger    *zap.Logger
	stats     *snitch.Stats
	allowCORS bool
}

func (h *LogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	userTags := map[string]string{}

	h.next.ServeHTTP(logw, r.WithContext(context.WithValue(ctx, statsTagskey, userTags)))

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

	for k, v := range userTags {
		tags[k] = v
	}

	h.increment("request.count", tags)
	h.valueAdd("request.duration", tags, float64(d.Nanoseconds())/float64(time.Millisecond))
}

func AddStatsMap(r *http.Request, tags map[string]string) {
	t, ok := r.Context().Value(statsTagskey).(map[string]string)
	if ok {
		for k, v := range tags {
			t[k] = v
		}
	}
}

func (h *LogHandler) increment(metric string, tags map[string]string) {
	err := h.stats.Increment(metric, tags, "@every 1m", false, true)
	if err != nil {
		h.logger.Error(
			"",
			zap.Error(err),
			zap.String("package", "rip"),
			zap.String("func", "statsIncrement"),
			zap.String("metric", metric),
		)
	}
}

func (h *LogHandler) valueAdd(metric string, tags map[string]string, v float64) {
	err := h.stats.ValueAdd(metric, tags, "avg", "@every 1m", false, false, v)
	if err != nil {
		h.logger.Error(
			"",
			zap.Error(err),
			zap.String("package", "rip"),
			zap.String("func", "statsValueAdd"),
			zap.String("metric", metric),
		)
	}
}
