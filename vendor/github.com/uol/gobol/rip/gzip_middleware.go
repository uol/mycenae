package rip

import (
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
)

const (
	encodingGzip = "gzip"

	headerAcceptEncoding  = "Accept-Encoding"
	headerContentEncoding = "Content-Encoding"
	headerContentLength   = "Content-Length"
	headerVary            = "Vary"
	headerSecWebSocketKey = "Sec-WebSocket-Key"

	BestCompression    = gzip.BestCompression
	BestSpeed          = gzip.BestSpeed
	DefaultCompression = gzip.DefaultCompression
	NoCompression      = gzip.NoCompression
)

type GzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (w *GzipResponseWriter) Write(b []byte) (int, error) {
	return w.gz.Write(b)
}

func (w *GzipResponseWriter) WriteHeader(s int) {
	w.ResponseWriter.WriteHeader(s)
}

func (w *GzipResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func NewGzipMiddleware(level int, next http.Handler) *GzipHandler {
	h := &GzipHandler{
		next: next,
	}
	h.pool.New = func() interface{} {
		gz, err := gzip.NewWriterLevel(ioutil.Discard, level)
		if err != nil {
			panic(err)
		}
		return gz
	}
	return h
}

type GzipHandler struct {
	pool sync.Pool
	next http.Handler
}

func (h *GzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if !strings.Contains(r.Header.Get(headerAcceptEncoding), encodingGzip) {
		h.next.ServeHTTP(w, r)
		return
	}

	if len(r.Header.Get(headerSecWebSocketKey)) > 0 {
		h.next.ServeHTTP(w, r)
		return
	}

	if w.Header().Get(headerContentEncoding) == encodingGzip {
		h.next.ServeHTTP(w, r)
		return
	}

	gz := h.pool.Get().(*gzip.Writer)
	defer h.pool.Put(gz)
	gz.Reset(w)

	headers := w.Header()
	headers.Set(headerContentEncoding, encodingGzip)
	headers.Set(headerVary, headerAcceptEncoding)

	gzw := &GzipResponseWriter{
		ResponseWriter: w,
		gz:             gz,
	}

	h.next.ServeHTTP(gzw, r)

	w.Header().Del(headerContentLength)

	gz.Close()
}
