package telnetmgr

import (
	"crypto/tls"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/gobol/logh"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/telnetsrv"
	"github.com/uol/mycenae/lib/tsstats"
)

//
// Implements a telnet server manager
// author: rnojiri
//

// Manager - controls the telnet servers
type Manager struct {
	collector                         *collector.Collector
	logger                            *logh.ContextualLogger
	stats                             *tsstats.StatsTS
	terminate                         bool
	connectionBalanceCheckTimeout     time.Duration
	maxWaitForDropTelnetConnsInterval time.Duration
	maxWaitForOtherNodeConnsBalancing time.Duration
	connectionBalanceStarted          bool
	globalConfiguration               *structs.GlobalTelnetServerConfiguration
	sharedConnectionCounter           uint32
	haltBalancingProcess              uint32
	otherNodes                        []string
	numOtherNodes                     int
	httpListenPort                    int
	closeConnectionChannel            chan struct{}
	httpClient                        *http.Client
	servers                           []*telnetsrv.Server
}

// New - creates a new manager instance
func New(globalConfiguration *structs.GlobalTelnetServerConfiguration, httpListenPort int, collector *collector.Collector, stats *tsstats.StatsTS) (*Manager, error) {

	connectionBalanceCheckTimeoutDuration, err := time.ParseDuration(globalConfiguration.TelnetConnsBalanceCheckInterval)
	if err != nil {
		return nil, err
	}

	maxWaitForDropTelnetConnsIntervalDuration, err := time.ParseDuration(globalConfiguration.MaxWaitForDropTelnetConnsInterval)
	if err != nil {
		return nil, err
	}

	httpRequestTimeoutDuration, err := time.ParseDuration(globalConfiguration.HTTPRequestTimeout)
	if err != nil {
		return nil, err
	}

	maxWaitForOtherNodeConnsBalancingDuration, err := time.ParseDuration(globalConfiguration.MaxWaitForOtherNodeConnsBalancing)
	if err != nil {
		return nil, err
	}

	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	otherNodes := []string{}
	for _, node := range globalConfiguration.Nodes {
		if node != hostName {
			otherNodes = append(otherNodes, node)
		}
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: httpRequestTimeoutDuration,
	}

	return &Manager{
		connectionBalanceCheckTimeout:     connectionBalanceCheckTimeoutDuration,
		maxWaitForDropTelnetConnsInterval: maxWaitForDropTelnetConnsIntervalDuration,
		maxWaitForOtherNodeConnsBalancing: maxWaitForOtherNodeConnsBalancingDuration,
		connectionBalanceStarted:          false,
		collector:                         collector,
		logger:                            logh.CreateContextualLogger(constants.StringsPKG, "telnetmgr"),
		stats:                             stats,
		terminate:                         false,
		httpListenPort:                    httpListenPort,
		sharedConnectionCounter:           0,
		haltBalancingProcess:              0,
		globalConfiguration:               globalConfiguration,
		otherNodes:                        otherNodes,
		numOtherNodes:                     len(otherNodes),
		closeConnectionChannel:            make(chan struct{}, globalConfiguration.ConnectionCloseChannelSize),
		httpClient:                        httpClient,
		servers:                           []*telnetsrv.Server{},
	}, nil
}

// AddServer - adds a new server
func (manager *Manager) AddServer(serverConfiguration *structs.TelnetServerConfiguration, globalTelnetConfig *structs.GlobalTelnetServerConfiguration, telnetHandler telnetsrv.TelnetDataHandler) error {

	server, err := telnetsrv.New(
		serverConfiguration,
		globalTelnetConfig,
		&manager.sharedConnectionCounter,
		manager.globalConfiguration.MaxTelnetConnections,
		&manager.closeConnectionChannel,
		manager.collector,
		manager.stats,
		telnetHandler,
	)

	if err != nil {
		return err
	}

	err = server.Listen()
	if err != nil {
		return err
	}

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, "AddServer").Msgf("server created and listening: %s", serverConfiguration.ServerName)
	}

	manager.servers = append(manager.servers, server)

	if !manager.connectionBalanceStarted {

		manager.connectionBalanceStarted = true

		go manager.startConnectionBalancer()
	}

	return nil
}

// Shutdown - shuts down all servers
func (manager *Manager) Shutdown() {

	numServers := len(manager.servers)
	if numServers > 0 {

		for i := 0; i < numServers; i++ {

			manager.servers[i].Shutdown()
		}

		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, "Shutdown").Msg("all telnet servers were shut down")
		}
	}
}

const cFuncStartConnectionBalancer string = "startConnectionBalancer"

// startConnectionBalancer - starts the connection balancer
func (manager *Manager) startConnectionBalancer() {

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msg("starting the connection balance checks")
	}

	for {
		<-time.After(manager.connectionBalanceCheckTimeout)

		if manager.terminate {
			if logh.InfoEnabled {
				manager.logger.Info().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msg("terminating the connection balance check")
			}
			return
		}

		if manager.numOtherNodes == 0 {
			if logh.InfoEnabled {
				manager.logger.Info().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msg("there are no other nodes to balance the connections")
			}
			return
		}

		var wg sync.WaitGroup
		wg.Add(manager.numOtherNodes)

		results := make([]uint32, manager.numOtherNodes)

		for i, node := range manager.otherNodes {
			manager.getNumConnectionsFromNode(node, &results[i], &wg)
		}

		wg.Wait()

		curConns := atomic.LoadUint32(&manager.sharedConnectionCounter)

		var sum uint32
		var stopBalancing bool

		for i := 0; i < manager.numOtherNodes; i++ {

			if curConns < results[i] {
				if logh.InfoEnabled {
					manager.logger.Info().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msgf("there is another node with more connections: %s (%d)", manager.otherNodes[i], results[i])
				}
				stopBalancing = true
				break
			}

			sum += results[i]
		}

		if stopBalancing {
			continue
		}

		average := uint32(math.Ceil(float64(sum) / float64(manager.numOtherNodes)))
		diff := curConns - average

		if curConns > average && diff >= manager.globalConfiguration.MaxUnbalancedTelnetConnsPerNode {

			excess := diff - manager.globalConfiguration.MaxUnbalancedTelnetConnsPerNode

			if excess > 0 {

				if !manager.dropConnections(excess) {

					<-time.After(manager.maxWaitForOtherNodeConnsBalancing)

					if atomic.CompareAndSwapUint32(&manager.haltBalancingProcess, 1, 0) {
						if logh.InfoEnabled {
							manager.logger.Info().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msg("resuming the balancing process")
						}
					} else if logh.WarnEnabled {
						manager.logger.Warn().Str(constants.StringsFunc, cFuncStartConnectionBalancer).Msg("balancing process is already running, something went wrong...")
					}
				}
			}
		}
	}
}

const cFuncDropConnections string = "dropConnections"

// dropConnections - add connections to be dropped and halt all new connections
func (manager *Manager) dropConnections(excess uint32) bool {

	if atomic.LoadUint32(&manager.haltBalancingProcess) > 0 {
		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msg("telnet balancing process is halted, waiting...")
		}
		return false
	}

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msg("halting connection balancing on other nodes")
	}

	manager.haltBalancingOnOtherNodes()

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msgf("the number of telnet connections was exceeded by %d connections", excess)
	}

	var j uint32
	for j = 0; j < excess; j++ {
		if logh.DebugEnabled {
			manager.logger.Debug().Str(constants.StringsFunc, cFuncDropConnections).Msg("adding to close connection channel")
		}
		manager.closeConnectionChannel <- struct{}{}
	}

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msgf("waiting for connections to drop: %s", manager.maxWaitForDropTelnetConnsInterval)
	}

	numServers := len(manager.servers)
	for i := 0; i < numServers; i++ {
		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msgf("halting new connections on server: %s", manager.servers[i].GetName())
		}
		manager.servers[i].DenyNewConnections(true)
	}

	<-time.After(manager.maxWaitForDropTelnetConnsInterval)

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msg("draining close connection channel...")
	}

	breakLoop := false
	for {
		select {
		case <-(manager.closeConnectionChannel):
			if logh.DebugEnabled {
				manager.logger.Debug().Str(constants.StringsFunc, cFuncDropConnections).Msg("close connection channel drained")
			}
		default:
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}

	if logh.InfoEnabled {
		manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msgf("waiting time for dropping connections is done: %s", manager.maxWaitForDropTelnetConnsInterval)
	}

	for i := 0; i < numServers; i++ {
		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, cFuncDropConnections).Msgf("setting server to accept new connections: %s", manager.servers[i].GetName())
		}
		manager.servers[i].DenyNewConnections(false)
	}

	return true
}

const (
	cFuncGetNumConnectionsFromNode string = "getNumConnectionsFromNode"
	cNode                          string = "node"
)

// getNumConnectionsFromNode - does a HEAD request to get number of connections from another node
func (manager *Manager) getNumConnectionsFromNode(node string, result *uint32, wg *sync.WaitGroup) {

	defer wg.Done()

	if logh.DebugEnabled {
		manager.logger.Debug().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Msg("asking node for the number of connections...")
	}

	url := fmt.Sprintf("http://%s:%d/%s", node, manager.httpListenPort, CountConnsURI)

	resp, err := manager.httpClient.Head(url)
	if err != nil {
		if logh.ErrorEnabled {
			manager.logger.Error().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Err(err).Send()
		}
		return
	}

	if resp.StatusCode != http.StatusOK {
		if logh.ErrorEnabled {
			manager.logger.Error().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Msgf("error requesting node's header: %s", url)
		}
		return
	}

	if len(resp.Header[HTTPHeaderTotalConnections]) != 1 {
		if logh.ErrorEnabled {
			manager.logger.Error().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Msgf("unexpected array of values in header: '%s'", HTTPHeaderTotalConnections)
		}
		return
	}

	r, err := strconv.ParseUint(resp.Header[HTTPHeaderTotalConnections][0], 10, 32)
	if err != nil {
		if logh.ErrorEnabled {
			manager.logger.Error().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Err(err).Send()
		}
		return
	}

	if logh.DebugEnabled {
		manager.logger.Debug().Str(constants.StringsFunc, cFuncGetNumConnectionsFromNode).Str(cNode, node).Msgf("node has %d connections", r)
	}

	(*result) = uint32(r)

	return
}

const (
	cFuncHaltBalancingOnOtherNodes = "haltBalancingOnOtherNodes"
)

// haltBalancingOnOtherNodes - does a HEAD request to tell other nodes to halt the balancing
func (manager *Manager) haltBalancingOnOtherNodes() {

	for _, node := range manager.otherNodes {

		if logh.InfoEnabled {
			manager.logger.Info().Str(constants.StringsFunc, cFuncHaltBalancingOnOtherNodes).Str(cNode, node).Msg("notifying node to halt the balancing process")
		}

		url := fmt.Sprintf("http://%s:%d/%s", node, manager.httpListenPort, HaltConnsURI)

		resp, err := manager.httpClient.Head(url)
		if err != nil {
			if logh.ErrorEnabled {
				manager.logger.Error().Str(constants.StringsFunc, cFuncHaltBalancingOnOtherNodes).Str(cNode, node).Err(err).Send()
			}
			return
		}

		if resp.StatusCode == http.StatusProcessing {
			if logh.InfoEnabled {
				manager.logger.Info().Str(constants.StringsFunc, cFuncHaltBalancingOnOtherNodes).Str(cNode, node).Msg("node is already halting the balancing process")
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			if logh.InfoEnabled {
				manager.logger.Info().Str(constants.StringsFunc, cFuncHaltBalancingOnOtherNodes).Str(cNode, node).Msg("node was notified to halt the connection balancing")
			}
			continue
		}

		if logh.ErrorEnabled {
			manager.logger.Error().Str(constants.StringsFunc, cFuncHaltBalancingOnOtherNodes).Str(cNode, node).Msg("error requesting node's to halt the balancing process")
		}
	}

	return
}
