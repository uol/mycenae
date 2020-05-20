package rip

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/uol/logh"
)

type dummyKeyType int

const (
	statsTagskey            dummyKeyType = 1
	metricNetworkConnection string       = "network.connection"
	metricRequestCount      string       = "http.request.count"
	metricRequestDuration   string       = "http.request.duration"
	metricResponseSize      string       = "http.response.size"
	tagMethod               string       = "method"
	tagStatus               string       = "status"
	tagPath                 string       = "path"
	strUndefined            string       = "undefined"
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

// LogHandler - add statistics from requests
type LogHandler struct {
	next           http.Handler
	stats          StatisticsInterface
	connectionTags []interface{}
	logger         *logh.ContextualLogger
}

// NewLogMiddleware - creates a new instance of LogHandler
func NewLogMiddleware(next http.Handler, port int, statisticsImpl StatisticsInterface) *LogHandler {

	return &LogHandler{
		next:  next,
		stats: statisticsImpl,
		connectionTags: []interface{}{
			"type", "tcp",
			"port", port,
			"source", "http",
		},
		logger: logh.CreateContextualLogger("pkg", "rip"),
	}
}

// StatisticsInterface - defines an interface to input request statistics
type StatisticsInterface interface {

	// Increment - increments a metric
	Increment(metric string, tags ...interface{})

	// Maximum - input a maximum operation
	Maximum(metric string, value float64, tags ...interface{})
}

// ServeHTTP - implements the interface to serve http requests
func (h *LogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	go h.stats.Increment(metricNetworkConnection, h.connectionTags...)

	start := time.Now()

	logResponseWriter := &LogResponseWriter{
		ResponseWriter: w,
	}

	ctx := context.Background()

	var userTags []interface{}

	h.next.ServeHTTP(logResponseWriter, r.WithContext(context.WithValue(ctx, statsTagskey, &userTags)))

	status := logResponseWriter.status

	tags := []interface{}{
		tagMethod, r.Method,
		tagStatus, strconv.Itoa(status),
	}

	tagsOK := true
	customPathFound := false
	userTags, ok := r.Context().Value(statsTagskey).([]interface{})
	if ok {

		numTags := len(userTags)
		tagsOK = numTags%2 == 0

		if tagsOK {

			for i := 0; i < numTags; i++ {

				if !customPathFound && i%2 != 0 {
					if value, ok := tags[i].(string); ok {
						if value == tagPath {
							customPathFound = true
						}
					}
				}

				tags = append(tags, userTags[i])
			}
		}
	}

	if tagsOK && !customPathFound {

		var uri string

		if status != 404 && status != 405 {

			i := strings.IndexByte(r.RequestURI, '?')
			var length int
			if i < 0 {
				length = len(r.RequestURI)
			} else {
				length = i
			}

			uri = r.RequestURI[:length]

		} else {
			uri = strUndefined
		}

		tags = append(tags, tagPath, uri)
	}

	d := time.Since(start)

	if tagsOK {
		h.stats.Increment(metricRequestCount, tags...)
		h.stats.Maximum(metricRequestDuration, float64(d.Nanoseconds())/float64(time.Millisecond), tags...)
		h.stats.Maximum(metricResponseSize, (float64)(logResponseWriter.size), tags...)
	} else {
		if logh.WarnEnabled {
			h.logger.Warn().Msgf("received a wrong number of tags: %+v", userTags...)
		}
	}
}
