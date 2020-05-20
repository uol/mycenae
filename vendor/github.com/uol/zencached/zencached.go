package zencached

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/uol/logh"
)

//
// The memcached client main structure.
// @author rnojiri
//

// Configuration - has the main configuration
type Configuration struct {
	Nodes                 []Node
	NumConnectionsPerNode int
	TelnetConfiguration
}

// Zencached - declares the main structure
type Zencached struct {
	nodeTelnetConns    []chan *Telnet
	numNodeTelnetConns int
	configuration      *Configuration
	logger             *logh.ContextualLogger
	shuttingDown       uint32
	metricsCollector   MetricsCollector
	enableMetrics      bool
}

// New - creates a new instance
func New(configuration *Configuration, metricsCollector MetricsCollector) (*Zencached, error) {

	numNodes := len(configuration.Nodes)
	nodeTelnetConns := make([]chan *Telnet, numNodes)

	for i := 0; i < numNodes; i++ {

		channel := make(chan *Telnet, configuration.NumConnectionsPerNode)

		for c := 0; c < configuration.NumConnectionsPerNode; c++ {

			telnetConn, err := NewTelnet(&configuration.Nodes[i], &configuration.TelnetConfiguration)
			if err != nil {
				return nil, err
			}

			channel <- telnetConn
		}

		nodeTelnetConns[i] = channel
	}

	enableMetrics := metricsCollector != nil

	return &Zencached{
		nodeTelnetConns:    nodeTelnetConns,
		numNodeTelnetConns: numNodes,
		configuration:      configuration,
		logger:             logh.CreateContextualLogger("pkg", "zencached"),
		metricsCollector:   metricsCollector,
		enableMetrics:      enableMetrics,
	}, nil
}

// Shutdown - closes all connections
func (z *Zencached) Shutdown() {

	if atomic.LoadUint32(&z.shuttingDown) == 1 {
		if logh.InfoEnabled {
			z.logger.Info().Msg("already shutting down...")
		}
		return
	}

	if logh.InfoEnabled {
		z.logger.Info().Msg("shutting down...")
	}

	atomic.SwapUint32(&z.shuttingDown, 1)
	closed := 0
	for nodeIndex, nodeConns := range z.nodeTelnetConns {

		if logh.InfoEnabled {
			z.logger.Info().Msgf("closing node connections from index: %d", nodeIndex)
		}

		for i := 0; i < z.configuration.NumConnectionsPerNode; i++ {

			if logh.DebugEnabled {
				z.logger.Debug().Msg("closing connection...")
			}

			conn := <-nodeConns
			conn.Close()
			closed++

			if logh.DebugEnabled {
				z.logger.Debug().Msg("connection closed")
			}
		}
	}
}

// GetTelnetConnByNodeIndex - returns a telnet connection by node index
func (z *Zencached) GetTelnetConnByNodeIndex(index int) (telnetConn *Telnet) {

	if !z.enableMetrics {

		telnetConn = <-z.nodeTelnetConns[index]

	} else {

		start := time.Now()
		telnetConn = <-z.nodeTelnetConns[index]
		elapsedTime := time.Since(start)

		z.metricsCollector.Count(
			1,
			metricNodeDistribution,
			tagNodeName, telnetConn.GetHost(),
		)

		z.metricsCollector.Maximum(
			float64(elapsedTime.Milliseconds()),
			metricNodeConnAvailableTime,
			tagNodeName, telnetConn.GetHost(),
		)
	}

	return
}

// GetTelnetConnection - returns an idle telnet connection
func (z *Zencached) GetTelnetConnection(routerHash []byte, key []byte) (telnetConn *Telnet, index int) {

	if routerHash == nil {
		routerHash = key
	}

	if len(routerHash) == 0 {
		index = rand.Intn(z.numNodeTelnetConns)
	} else {
		index = int(routerHash[len(routerHash)-1]) % z.numNodeTelnetConns
	}

	telnetConn = z.GetTelnetConnByNodeIndex(index)

	return
}

// ReturnTelnetConnection - returns a telnet connection to the pool
func (z *Zencached) ReturnTelnetConnection(telnetConn *Telnet, index int) {

	z.nodeTelnetConns[index] <- telnetConn
}
