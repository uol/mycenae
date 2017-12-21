package collector

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func (collect *Collector) Scollector(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	points := TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	for _, point := range points {
		collect.handleRESTpacket(point, true)
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) Text(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	points := TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	for _, point := range points {
		go collect.handleRESTpacket(point, false)
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) handleRESTpacket(rcvMsg TSDBpoint, number bool) {

	if number {
		rcvMsg.Text = ""
	} else {
		rcvMsg.Value = nil
	}

	collect.HandlePacket(rcvMsg, number, "rest", nil)
}
