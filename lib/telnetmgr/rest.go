package telnetmgr

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// HaltTelnetBalancingProcess - tells to this node to halt any running balancing process
func (manager *Manager) HaltTelnetBalancingProcess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	lf := []zapcore.Field{
		zap.String("package", "telnetmgr"),
		zap.String("func", "HaltTelnetBalancingProcess"),
	}

	if atomic.CompareAndSwapUint32(&manager.haltBalancingProcess, 0, 1) {

		manager.logger.Info("halting the telnet connections balancing process", lf...)

		w.WriteHeader(http.StatusOK)
		return
	}

	manager.logger.Info("telnet connections balancing process is already halted", lf...)

	w.WriteHeader(http.StatusProcessing)

	return
}
