package collector

import (
	"compress/gzip"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
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

const (
	cFuncIPStats   string = "sendIPStats"
	cXForwardedFor string = "X-Forwarded-For"
)

func (collect *Collector) sendIPStats(r *http.Request) {

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		if logh.ErrorEnabled {
			collect.logger.Error().Str(constants.StringsFunc, cFuncIPStats).Err(err).Msg("error parsing remote address")
		}
		return
	}

	if ip == constants.StringsEmpty {
		array := strings.Split(r.Header.Get(cXForwardedFor), ",")
		if len(array) > 0 {
			ip = strings.TrimSpace(array[0])
		}
	}

	statsValueAdd("network.ip", map[string]string{"ip": ip, "source": "http"}, 1)
}
