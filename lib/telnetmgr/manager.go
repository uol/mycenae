package telnetmgr

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/telnetsrv"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//
// Implements a telnet server manager
// author: rnojiri
//

// Manager - controls the telnet servers
type Manager struct {
	collector                         *collector.Collector
	logger                            *zap.Logger
	stats                             *tsstats.StatsTS
	terminate                         bool
	connectionBalanceCheckTimeout     time.Duration
	maxWaitForDropTelnetConnsInterval time.Duration
	connectionBalanceStarted          bool
	globalConfiguration               *structs.GlobalTelnetServerConfiguration
	sharedConnectionCounter           uint32
	otherNodes                        []string
	numOtherNodes                     int
	httpListenPort                    int
	closeConnectionChannel            chan struct{}
	httpClient                        *http.Client
	servers                           []*telnetsrv.Server
}

// New - creates a new manager instance
func New(globalConfiguration *structs.GlobalTelnetServerConfiguration, httpListenPort int, collector *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger) (*Manager, error) {

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
		connectionBalanceStarted:          false,
		collector:                         collector,
		logger:                            logger,
		stats:                             stats,
		terminate:                         false,
		httpListenPort:                    httpListenPort,
		sharedConnectionCounter:           0,
		globalConfiguration:               globalConfiguration,
		otherNodes:                        otherNodes,
		numOtherNodes:                     len(otherNodes),
		closeConnectionChannel:            make(chan struct{}, globalConfiguration.ConnectionCloseChannelSize),
		httpClient:                        httpClient,
		servers:                           []*telnetsrv.Server{},
	}, nil
}

// AddServer - adds a new server
func (manager *Manager) AddServer(serverConfiguration *structs.TelnetServerConfiguration, telnetHandler telnetsrv.TelnetDataHandler) error {

	server, err := telnetsrv.New(
		serverConfiguration,
		&manager.sharedConnectionCounter,
		manager.globalConfiguration.MaxTelnetConnections,
		&manager.closeConnectionChannel,
		manager.collector,
		manager.stats,
		manager.logger,
		telnetHandler,
	)

	if err != nil {
		return err
	}

	err = server.Listen()
	if err != nil {
		return err
	}

	lf := []zapcore.Field{
		zap.String("package", "telnetmgr"),
		zap.String("func", "AddServer"),
	}

	manager.logger.Info(fmt.Sprintf("server created and listening: %s", serverConfiguration.ServerName), lf...)

	manager.servers = append(manager.servers, server)

	if !manager.connectionBalanceStarted {

		go manager.startConnectionBalancer()
	}

	return nil
}

// Shutdown - shuts down all servers
func (manager *Manager) Shutdown() {

	numServers := len(manager.servers)
	if numServers > 0 {

		lf := []zapcore.Field{
			zap.String("package", "telnetmgr"),
			zap.String("func", "Shutdown"),
		}

		for i := 0; i < numServers; i++ {

			manager.servers[i].Shutdown()
		}

		manager.logger.Info("all telnet servers were shut down", lf...)
	}
}

// startConnectionBalancer - starts the connection balancer
func (manager *Manager) startConnectionBalancer() {

	lf := []zapcore.Field{
		zap.String("package", "telnetmgr"),
		zap.String("func", "StartConnectionBalancer"),
	}

	manager.logger.Info("starting the connection balance checks", lf...)

	manager.connectionBalanceStarted = true

	for {
		<-time.After(manager.connectionBalanceCheckTimeout)

		if manager.terminate {
			manager.logger.Info("terminating the connection balance check", lf...)
			return
		}

		if manager.numOtherNodes == 0 {
			manager.logger.Info("there are no other nodes to balance the connections", lf...)
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

		for i := 0; i < manager.numOtherNodes; i++ {

			if curConns > results[i] && curConns-results[i] >= manager.globalConfiguration.MaxUnbalancedTelnetConnsPerNode {

				excess := curConns - results[i] - manager.globalConfiguration.MaxUnbalancedTelnetConnsPerNode

				manager.dropConnections(excess)

				break
			}
		}
	}
}

// dropConnections - add connections to be dropped and halt all new connections
func (manager *Manager) dropConnections(excess uint32) {

	lf := []zapcore.Field{
		zap.String("package", "telnetmgr"),
		zap.String("func", "dropConnections"),
	}

	manager.logger.Info(fmt.Sprintf("the number of telnet connections was exceeded by %d connections", excess), lf...)

	var j uint32
	for j = 0; j < excess; j++ {
		manager.logger.Debug("adding to close connection channel", lf...)
		manager.closeConnectionChannel <- struct{}{}
	}

	manager.logger.Info(fmt.Sprintf("waiting for connections to drop: %s", manager.maxWaitForDropTelnetConnsInterval), lf...)

	numServers := len(manager.servers)
	for i := 0; i < numServers; i++ {
		manager.logger.Info(fmt.Sprintf("halting new connections on server: %s", manager.servers[i].GetName()), lf...)
		manager.servers[i].DenyNewConnections(true)
	}

	<-time.After(manager.maxWaitForDropTelnetConnsInterval)

	manager.logger.Info("draining close connection channel...", lf...)

	breakLoop := false
	for {
		select {
		case <-(manager.closeConnectionChannel):
			manager.logger.Debug("close connection channel drained", lf...)
		default:
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}

	manager.logger.Info(fmt.Sprintf("waiting time for dropping connections is done: %s", manager.maxWaitForDropTelnetConnsInterval), lf...)

	for i := 0; i < numServers; i++ {
		manager.logger.Info(fmt.Sprintf("setting server to accept new connections: %s", manager.servers[i].GetName()), lf...)
		manager.servers[i].DenyNewConnections(false)
	}
}

// getNumConnectionsFromNode - does a HEAD request to get number of connections from another node
func (manager *Manager) getNumConnectionsFromNode(node string, result *uint32, wg *sync.WaitGroup) {

	defer wg.Done()

	lf := []zapcore.Field{
		zap.String("package", "telnetmgr"),
		zap.String("func", "getNumConnectionsFromNode"),
		zap.String("node", node),
	}

	manager.logger.Debug(fmt.Sprintf("asking node for the number of connections..."), lf...)

	url := fmt.Sprintf("http://%s:%d/%s", node, manager.httpListenPort, URI)

	resp, err := manager.httpClient.Head(url)
	if err != nil {
		manager.logger.Error(err.Error(), lf...)
		return
	}

	if resp.StatusCode != http.StatusOK {
		manager.logger.Error(fmt.Sprintf("error requesting node's header: %s", url), lf...)
		return
	}

	if len(resp.Header[HTTPHeaderTotalConnections]) != 1 {
		manager.logger.Error(fmt.Sprintf("unexpected array of values in header '%s'", HTTPHeaderTotalConnections), lf...)
		return
	}

	r, err := strconv.ParseUint(resp.Header[HTTPHeaderTotalConnections][0], 10, 32)
	if err != nil {
		manager.logger.Error(err.Error(), lf...)
		return
	}

	manager.logger.Debug(fmt.Sprintf("node has %d connections", r), lf...)

	(*result) = uint32(r)

	return
}
