package collector

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (collect *Collector) handle(w http.ResponseWriter, r *http.Request, number bool) {

	points := TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	hasError := false

	for _, point := range points {
		gerr = collect.handleRESTpacket(point, number)

		if gerr != nil {
			rip.Fail(w, gerr)
			hasError = true
			break
		}
	}

	if !hasError {
		rip.Success(w, http.StatusNoContent, nil)
	}

	return
}

func (collect *Collector) HandleNumber(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	collect.sendIPStats(r)
	collect.handle(w, r, true)
}

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

func (collect *Collector) handleRESTpacket(rcvMsg TSDBpoint, number bool) gobol.Error {

	if number {
		rcvMsg.Text = ""
	} else {
		rcvMsg.Value = nil
	}

	p := &Point{}

	err := collect.MakePacket(p, rcvMsg, number)

	if err != nil {
		return err
	}

	collect.HandlePacket(rcvMsg, p, number, "rest", nil)

	return nil
}
