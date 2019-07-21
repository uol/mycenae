package collector

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collect *Collector) handle(w http.ResponseWriter, r *http.Request, number bool) {

	var bytes []byte
	var err error
	var gzipReader *gzip.Reader

	if r.Header.Get("Content-Encoding") == "gzip" {

		gzipReader, err = gzip.NewReader(r.Body)
		if err != nil {
			rip.Fail(w, errUnmarshal("handle", err))
			return
		}

		bytes, err = ioutil.ReadAll(gzipReader)

	} else {

		bytes, err = ioutil.ReadAll(r.Body)
	}

	if err != nil {
		rip.Fail(w, errUnmarshal("handle", err))
		return
	}

	_, gerr := collect.HandleJSONBytes(bytes, "http", number)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)

	if gzipReader != nil {
		gzipReader.Close()
	}

	r.Body.Close()

	return
}

// HandleNumber - handles the point in number format
func (collect *Collector) HandleNumber(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	collect.sendIPStats(r)
	collect.handle(w, r, true)
}

// HandleText - handles the point in text format
func (collect *Collector) HandleText(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	collect.sendIPStats(r)
	collect.handle(w, r, false)
}

var sendIPStatsLogFields = []zapcore.Field{
	zap.String("package", "collect"),
	zap.String("func", "sendIPStats"),
}

func (collect *Collector) sendIPStats(r *http.Request) {

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		gblog.Error(fmt.Sprintf("error parsing remote address: %s", err.Error()), sendIPStatsLogFields...)
		return
	}

	if ip == "" {
		array := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
		if len(array) > 0 {
			ip = strings.TrimSpace(array[0])
		}
	}

	statsValueAdd("network.ip", map[string]string{"ip": ip, "source": "http"}, 1)
}
