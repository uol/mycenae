package collector

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"
)

func (collect *Collector) Scollector(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	points := TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	returnPoints := RestErrors{}

	restChan := make(chan RestError, len(points))

	for _, point := range points {
		collect.concPoints <- struct{}{}
		go collect.handleRESTpacket(point, true, restChan)

	}

	for range points {
		re := <-restChan
		if re.Gerr != nil {

			gblog.WithFields(re.Gerr.LogFields()).Error(re.Gerr.Error())

			ks := "default"
			if v, ok := re.Datapoint.Tags["ksid"]; ok {
				ks = v
			}

			statsPointsError(ks, "number")

			reu := RestErrorUser{
				Datapoint: re.Datapoint,
				Error:     re.Gerr.Message(),
			}

			returnPoints.Errors = append(returnPoints.Errors, reu)

		} else {
			statsPoints(re.Datapoint.Tags["ksid"], "number")
		}
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
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

	returnPoints := RestErrors{}

	restChan := make(chan RestError, len(points))

	for _, point := range points {
		collect.concPoints <- struct{}{}
		go collect.handleRESTpacket(point, false, restChan)
	}

	var reqKS string
	var numKS int

	for range points {
		re := <-restChan
		if re.Gerr != nil {

			ks := "default"
			if v, ok := re.Datapoint.Tags["ksid"]; ok {
				ks = v
			}
			if ks != reqKS {
				reqKS = ks
				numKS++
			}

			statsPointsError(ks, "text")

			reu := RestErrorUser{
				Datapoint: re.Datapoint,
				Error:     re.Gerr.Message(),
			}

			returnPoints.Errors = append(returnPoints.Errors, reu)
		} else {
			pks := re.Datapoint.Tags["ksid"]
			if pks != reqKS {
				reqKS = pks
				numKS++
			}
			statsPoints(re.Datapoint.Tags["ksid"], "text")
		}
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) handleRESTpacket(rcvMsg TSDBpoint, number bool, restChan chan RestError) {
	recvPoint := rcvMsg
	var gerr gobol.Error

	if number {
		rcvMsg.Text = ""
	} else {
		rcvMsg.Value = nil
	}

	gerr = collect.HandlePacket(rcvMsg, number)

	restChan <- RestError{
		Datapoint: recvPoint,
		Gerr:      gerr,
	}

	<-collect.concPoints
}
