package collector

import (
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
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

func (collect *Collector) HandleNumberTelnetFormat(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	defer r.Body.Close()
	if r.Header.Get("Content-Type") != "text/plain" {
		rip.Fail(w, errValidationTelnet("Content-Type must be text/plain"))
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		rip.Fail(w, errValidationTelnet(err.Error()))
		return
	}

	body := string(b[:])

	point, gerr := collect.validateTelnetFormat(body)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	gerr = collect.handleRESTpacket(point, true)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) HandleNumber(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	collect.handle(w, r, true)
}

func (collect *Collector) HandleText(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	collect.handle(w, r, false)
}

func (collect *Collector) handleRESTpacket(rcvMsg TSDBpoint, number bool) gobol.Error {

	if number {
		rcvMsg.Text = ""
	} else {
		rcvMsg.Value = nil
	}

	p := &Point{}

	err := collect.makePacket(p, rcvMsg, number)

	if err != nil {
		return err
	}

	collect.HandlePacket(rcvMsg, p, number, "rest", nil)

	return nil
}
