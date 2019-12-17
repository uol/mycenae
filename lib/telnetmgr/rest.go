package telnetmgr

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/uol/gobol/logh"
	"github.com/uol/mycenae/lib/constants"

	"github.com/julienschmidt/httprouter"
)

//
// Has some methods to collect information about the telnet connections
// author: rnojiri
//

// CountConnsURI - the uri to the get node connections
const CountConnsURI string = "node/connections"

// HaltConnsURI - the uri to
const HaltConnsURI string = "node/halt/balancing"

// HTTPHeaderTotalConnections - the header name to set the total tasks number
const HTTPHeaderTotalConnections string = "X-Total-Connections"

// CountConnections - returns the number of telnet connections from this node
func (manager *Manager) CountConnections(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	w.Header().Add(HTTPHeaderTotalConnections, fmt.Sprintf("%d", atomic.LoadUint32(&manager.sharedConnectionCounter)))
	w.WriteHeader(http.StatusOK)

	return
}

const (
	cFuncHaltTelnetBalancingProcess string = "HaltTelnetBalancingProcess"
)

// HaltTelnetBalancingProcess - tells to this node to halt any running balancing process
func (manager *Manager) HaltTelnetBalancingProcess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	if atomic.CompareAndSwapUint32(&manager.haltBalancingProcess, 0, 1) {

		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, cFuncHaltTelnetBalancingProcess).Msg("halting the telnet connections balancing process")
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncHaltTelnetBalancingProcess).Msg("telnet connections balancing process is already halted")
	}

	w.WriteHeader(http.StatusProcessing)

	return
}
