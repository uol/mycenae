package telnetmgr

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
)

//
// Has some methods to collect information about the telnet connections
// author: rnojiri
//

// URI - the uri from the get connections
const URI string = "node/connections"

// HTTPHeaderTotalConnections - the header name to set the total tasks number
const HTTPHeaderTotalConnections string = "X-Total-Connections"

// CountConnections - returns the number of telnet connections from this node
func (manager *Manager) CountConnections(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	w.Header().Add(HTTPHeaderTotalConnections, fmt.Sprintf("%d", atomic.LoadUint32(&manager.sharedConnectionCounter)))
	w.WriteHeader(http.StatusOK)

	return
}
