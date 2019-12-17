package election

import (
	"fmt"
	"os"
	"time"

	"github.com/uol/gobol/logh"

	"github.com/uol/gobol/util"

	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

//
// A zookeeper cluster election manager
// author: rnojiri
//

const defaultChannelSize int = 5

// Manager - handles the zookeeper election
type Manager struct {
	zkConnection                   *zk.Conn
	config                         *Config
	isMaster                       bool
	defaultACL                     []zk.ACL
	logger                         *logh.ContextualLogger
	feedbackChannel                chan int
	clusterConnectionEventChannel  <-chan zk.Event
	sessionID                      int64
	nodeName                       string
	clusterNodes                   sync.Map
	terminate                      bool
	sessionTimeoutDuration         time.Duration
	reconnectionTimeoutDuration    time.Duration
	clusterChangeCheckTimeDuration time.Duration
	clusterChangeWaitTimeDuration  time.Duration
}

// New - creates a new instance
func New(config *Config) (*Manager, error) {

	sessionTimeoutDuration, err := time.ParseDuration(config.SessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid session timeout duration: %s", config.SessionTimeout)
	}

	reconnectionTimeoutDuration, err := time.ParseDuration(config.ReconnectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid reconnection timeout duration: %s", config.ReconnectionTimeout)
	}

	clusterChangeCheckTimeDuration, err := time.ParseDuration(config.ClusterChangeCheckTime)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster change check time duration: %s", config.ClusterChangeCheckTime)
	}

	clusterChangeWaitTimeDuration, err := time.ParseDuration(config.ClusterChangeWaitTime)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster change wait time duration: %s", config.ClusterChangeWaitTime)
	}

	return &Manager{
		zkConnection:                   nil,
		config:                         config,
		defaultACL:                     zk.WorldACL(zk.PermAll),
		logger:                         logh.CreateContextualLogger("pkg", "election"),
		feedbackChannel:                make(chan int, defaultChannelSize),
		clusterConnectionEventChannel:  nil,
		clusterNodes:                   sync.Map{},
		terminate:                      false,
		sessionTimeoutDuration:         sessionTimeoutDuration,
		reconnectionTimeoutDuration:    reconnectionTimeoutDuration,
		clusterChangeCheckTimeDuration: clusterChangeCheckTimeDuration,
		clusterChangeWaitTimeDuration:  clusterChangeWaitTimeDuration,
	}, nil
}

// getNodeData - check if node exists
func (m *Manager) getNodeData(node string) (*string, error) {

	data, _, err := m.zkConnection.Get(node)

	exists := true
	if err != nil {
		if err.Error() == "zk: node does not exist" {
			exists = false
		} else {
			return nil, err
		}
	}

	if !exists {
		return nil, nil
	}

	result := string(data)

	return &result, nil
}

// getZKMasterNode - returns zk master node name
func (m *Manager) getZKMasterNode() (*string, error) {

	if m.zkConnection == nil {
		return nil, nil
	}

	data, err := m.getNodeData(m.config.ZKElectionNodeURI)
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Err(err).Str("func", "getZKMasterNode").Msg("error retrieving ZK election node data")
		}
		return nil, err
	}

	return data, nil
}

// connect - connects to the zookeeper
func (m *Manager) connect() error {

	if logh.InfoEnabled {
		m.logger.Info().Str("func", "connect").Msg("connecting to zookeeper...")
	}

	var err error

	// Create the ZK connection
	m.zkConnection, m.clusterConnectionEventChannel, err = zk.Connect(m.config.ZKURL, m.sessionTimeoutDuration)
	if err != nil {
		return err
	}

	go func() {
		for {

			if m.terminate {
				if logh.InfoEnabled {
					m.logger.Info().Str("func", "connect").Msg("ending cluster connection event loop")
				}
				m.feedbackChannel <- Disconnected
				return
			}

			event := <-m.clusterConnectionEventChannel
			if event.Type == zk.EventSession {
				if event.State == zk.StateConnected ||
					event.State == zk.StateConnectedReadOnly {
					if logh.InfoEnabled {
						m.logger.Info().Str("func", "connect").Msg("connection established with zookeeper")
					}
				} else if event.State == zk.StateSaslAuthenticated ||
					event.State == zk.StateHasSession {
					if logh.InfoEnabled {
						m.logger.Info().Str("func", "connect").Msg("session created in zookeeper")
					}
				} else if event.State == zk.StateAuthFailed ||
					event.State == zk.StateDisconnected ||
					event.State == zk.StateExpired {
					if logh.InfoEnabled {
						m.logger.Info().Str("func", "connect").Msg("zookeeper connection was lost")
					}
					m.Disconnect()
					m.feedbackChannel <- Disconnected
					for {
						<-time.After(m.reconnectionTimeoutDuration)
						m.zkConnection, m.clusterConnectionEventChannel, err = zk.Connect(m.config.ZKURL, m.sessionTimeoutDuration)
						if err != nil {
							if logh.ErrorEnabled {
								m.logger.Error().Str("func", "connect").Err(err).Msg("error reconnecting to zookeeper")
							}
						} else {
							_, err := m.Start()
							if err != nil {
								if logh.ErrorEnabled {
									m.logger.Error().Str("func", "connect").Err(err).Msg("error starting election loop")
								}
							} else {
								return
							}
						}
					}
				}
			}
		}
	}()

	return nil
}

// Start - starts to listen zk events
func (m *Manager) Start() (*chan int, error) {

	m.terminate = false

	err := m.connect()
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "Start").Err(err).Msg("error connecting to zookeeper")
		}
		return nil, err
	}

	err = m.electForMaster()
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "Start").Err(err).Msg("error electing this node for master")
		}
		return nil, err
	}

	err = m.createSlaveDir("Start")
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "Start").Err(err).Msg("error creating slave directory")
		}
		return nil, err
	}

	err = m.listenForElectionEvents()
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "Start").Err(err).Msg("error listening for zk election node events")
		}
		return nil, err
	}

	err = m.listenForNodeEvents()
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "Start").Err(err).Msg("error listening for zk slave node events")
		}
		return nil, err
	}

	return &m.feedbackChannel, nil
}

// listenForElectionEvents - starts to listen for election node events
func (m *Manager) listenForElectionEvents() error {

	_, _, electionEventsChannel, err := m.zkConnection.ExistsW(m.config.ZKElectionNodeURI)
	if err != nil {
		return err
	}

	go func() {
		for {

			if m.terminate {
				if logh.InfoEnabled {
					m.logger.Info().Str("func", "listenForElectionEvents").Msg("ending election events loop")
				}
				m.feedbackChannel <- Disconnected
				return
			}

			event := <-electionEventsChannel
			if event.Type == zk.EventNodeDeleted {
				if logh.InfoEnabled {
					m.logger.Info().Str("func", "listenForElectionEvents").Msg("master has quit, trying to be the new master...")
				}
				err := m.electForMaster()
				if err != nil {
					if logh.ErrorEnabled {
						m.logger.Error().Str("func", "listenForElectionEvents").Err(err).Msg("error trying to elect this node for master")
					}
				}
			} else if event.Type == zk.EventNodeCreated {
				if logh.InfoEnabled {
					m.logger.Info().Str("func", "listenForElectionEvents").Msg("a new master has been elected...")
				}
			}
		}
	}()

	return nil
}

// listenForNodeEvents - starts to listen for node events
// Note: the zkConnection.ExistsW(...) and zkConnection.ChildrenW(...) does not work in the expected way, so I'm doing this manually
func (m *Manager) listenForNodeEvents() error {

	cluster, err := m.GetClusterInfo()
	if err != nil {
		return err
	}

	for _, node := range cluster.Nodes {
		m.clusterNodes.Store(node, true)
	}

	go func() {
		for {

			if m.terminate {
				if logh.InfoEnabled {
					m.logger.Info().Str("func", "listenForNodeEvents").Msg("ending node events loop")
				}
				m.feedbackChannel <- Disconnected
				return
			}

			<-time.After(m.clusterChangeCheckTimeDuration)

			cluster, err := m.GetClusterInfo()
			if err != nil {
				if logh.ErrorEnabled {
					m.logger.Error().Str("func", "listenForNodeEvents").Err(err).Send()
				}
			} else {
				changed := false
				if len(cluster.Nodes) != util.GetSyncMapSize(&m.clusterNodes) {
					changed = true
				} else {
					for _, node := range cluster.Nodes {
						if _, ok := m.clusterNodes.Load(node); !ok {
							changed = true
							break
						}
					}
				}

				if changed {
					if logh.InfoEnabled {
						m.logger.Info().Str("func", "listenForNodeEvents").Msg("cluster node configuration changed")
					}
					m.clusterNodes.Range(func(k, _ interface{}) bool {
						m.clusterNodes.Delete(k)
						return true
					})
					for _, node := range cluster.Nodes {
						m.clusterNodes.Store(node, true)
					}
					m.feedbackChannel <- ClusterChanged
					<-time.After(m.clusterChangeWaitTimeDuration)
				}
			}
		}
	}()

	return nil
}

// Disconnect - disconnects from the zookeeper
func (m *Manager) Disconnect() {

	m.terminate = true
	if m.zkConnection != nil && m.zkConnection.State() != zk.StateDisconnected {
		m.zkConnection.Close()
		m.feedbackChannel <- Disconnected
		time.Sleep(2 * time.Second)
		if logh.InfoEnabled {
			m.logger.Info().Str("func", "Disconnect").Msg("zk connection closed")
		}
	} else {
		if logh.InfoEnabled {
			m.logger.Info().Str("func", "Disconnect").Msg("zk connection is already closed")
		}
	}
}

// GetHostname - retrieves this node hostname from the OS
func (m *Manager) GetHostname() (string, error) {

	name, err := os.Hostname()
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "GetHostname").Err(err).Msg("could not retrive this node hostname")
		}
		return "", err
	}

	return name, nil
}

// createSlaveDir - creates the slave directory
func (m *Manager) createSlaveDir(funcName string) error {

	data, err := m.getNodeData(m.config.ZKSlaveNodesURI)
	if err != nil {
		return err
	}

	if data == nil {
		path, err := m.zkConnection.Create(m.config.ZKSlaveNodesURI, nil, int32(0), m.defaultACL)
		if err != nil {
			if logh.ErrorEnabled {
				m.logger.Error().Str("func", funcName).Err(err).Msg("error creating slave node directory")
			}
			return err
		}

		if logh.InfoEnabled {
			m.logger.Info().Str("func", funcName).Msg("slave node directory created: " + path)
		}
	}

	return nil
}

// registerAsSlave - register this node as a slave
func (m *Manager) registerAsSlave(nodeName string) error {

	err := m.createSlaveDir("registerAsSlave")
	if err != nil {
		return err
	}

	slaveNode := m.config.ZKSlaveNodesURI + "/" + nodeName

	data, err := m.getNodeData(slaveNode)
	if err != nil {
		return err
	}

	if data == nil {
		path, err := m.zkConnection.Create(slaveNode, []byte(nodeName), int32(zk.FlagEphemeral), m.defaultACL)
		if err != nil {
			if logh.ErrorEnabled {
				m.logger.Error().Str("func", "registerAsSlave").Err(err).Msg("error creating a slave node")
			}
			return err
		}

		if logh.InfoEnabled {
			m.logger.Info().Str("func", "registerAsSlave").Msg("slave node created: " + path)
		}
	} else {
		if logh.InfoEnabled {
			m.logger.Info().Str("func", "registerAsSlave").Msg("slave node already exists: " + slaveNode)
		}
	}

	m.isMaster = false
	m.feedbackChannel <- Slave

	return nil
}

// electForMaster - try to elect this node as the master
func (m *Manager) electForMaster() error {

	name, err := m.GetHostname()
	if err != nil {
		return err
	}

	zkMasterNode, err := m.getZKMasterNode()
	if err != nil {
		return err
	}

	if zkMasterNode != nil {
		if name == *zkMasterNode {
			if logh.InfoEnabled {
				m.logger.Info().Str("func", "electForMaster").Msg("this node is the master: " + *zkMasterNode)
			}
			m.isMaster = true
		} else {
			if logh.InfoEnabled {
				m.logger.Info().Str("func", "electForMaster").Msg("another node is the master: " + *zkMasterNode)
			}
			return m.registerAsSlave(name)
		}
	}

	path, err := m.zkConnection.Create(m.config.ZKElectionNodeURI, []byte(name), int32(zk.FlagEphemeral), m.defaultACL)
	if err != nil {
		if err.Error() == "zk: node already exists" {
			if logh.InfoEnabled {
				m.logger.Info().Str("func", "electForMaster").Msg("some node has became master before this node")
			}
			return m.registerAsSlave(name)
		}

		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "electForMaster").Err(err).Msg("error creating node")
		}

		return err
	}

	if logh.InfoEnabled {
		m.logger.Info().Str("func", "electForMaster").Msg("master node created: " + path)
	}

	m.isMaster = true
	m.feedbackChannel <- Master

	slaveNode := m.config.ZKSlaveNodesURI + "/" + name
	slave, err := m.getNodeData(slaveNode)
	if err != nil {
		if logh.ErrorEnabled {
			m.logger.Error().Str("func", "electForMaster").Err(err).Msgf("error retrieving a slave node data '%s'", slaveNode)
		}
		return nil
	}

	if slave != nil {
		err = m.zkConnection.Delete(slaveNode, 0)
		if err != nil {
			if logh.ErrorEnabled {
				m.logger.Error().Str("func", "electForMaster").Err(err).Msgf("error deleting slave node '%s'", slaveNode)
			}
		} else {
			if logh.InfoEnabled {
				m.logger.Info().Str("func", "electForMaster").Msg("slave node deleted: " + slaveNode)
			}
		}
	}

	return nil
}

// IsMaster - check if the cluster is the master
func (m *Manager) IsMaster() bool {
	return m.isMaster
}

// GetClusterInfo - return cluster info
func (m *Manager) GetClusterInfo() (*Cluster, error) {

	if m.zkConnection == nil {
		return nil, nil
	}

	nodes := []string{}
	masterNode, err := m.getZKMasterNode()
	if err != nil {
		return nil, err
	}

	if masterNode != nil {
		nodes = append(nodes, *masterNode)
	}

	slaveDir, err := m.getNodeData(m.config.ZKSlaveNodesURI)
	if err != nil {
		return nil, err
	}

	var children []string
	if slaveDir != nil {
		children, _, err = m.zkConnection.Children(m.config.ZKSlaveNodesURI)
		if err != nil {
			if logh.ErrorEnabled {
				m.logger.Error().Str("func", "GetClusterInfo").Err(err).Msg("error getting slave nodes")
			}
			return nil, err
		}

		nodes = append(nodes, children...)
	} else {
		children = []string{}
	}

	cluster := &Cluster{
		IsMaster: m.isMaster,
		Slaves:   children,
		Nodes:    nodes,
		NumNodes: len(nodes),
	}

	if masterNode != nil {
		cluster.Master = *masterNode
	}

	return cluster, nil
}
