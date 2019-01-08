package election

import (
	"fmt"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
)

const defaultChannelSize int = 5
const terminalChannelSize int = 2

// Manager - handles the zookeeper election
type Manager struct {
	zkConnection      *zk.Conn
	config            *Config
	isMaster          bool
	defaultACL        []zk.ACL
	logger            *zap.Logger
	electionChannel   chan int
	connectionChannel <-chan zk.Event
	messageChannel    chan int
	terminateChannel  chan bool
	sessionID         int64
	nodeName          string
}

// New - creates a new instance
func New(config *Config, logger *zap.Logger) (*Manager, error) {

	return &Manager{
		zkConnection:      nil,
		config:            config,
		defaultACL:        zk.WorldACL(zk.PermAll),
		logger:            logger,
		electionChannel:   make(chan int, defaultChannelSize),
		messageChannel:    make(chan int, defaultChannelSize),
		terminateChannel:  make(chan bool, terminalChannelSize),
		connectionChannel: nil,
	}, nil
}

// getNodeData - check if node exists
func (e *Manager) getNodeData(node string) (*string, error) {

	data, _, err := e.zkConnection.Get(node)

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
func (e *Manager) getZKMasterNode() (*string, error) {

	if e.zkConnection == nil {
		return nil, nil
	}

	data, err := e.getNodeData(e.config.ZKElectionNodeURI)
	if err != nil {
		e.logError("getZKMasterNode", "error retrieving ZK election node data")
		return nil, err
	}

	return data, nil
}

// connect - connects to the zookeeper
func (e *Manager) connect() error {

	e.logInfo("connect", "connecting to zookeeper...")

	var err error

	// Create the ZK connection
	e.zkConnection, e.connectionChannel, err = zk.Connect(e.config.ZKURL, time.Duration(e.config.SessionTimeout)*time.Second)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case event := <-e.connectionChannel:
				if event.Type == zk.EventSession {
					if event.State == zk.StateConnected ||
						event.State == zk.StateConnectedReadOnly {
						e.logInfo("connect", "connection established with zookeeper")
					} else if event.State == zk.StateSaslAuthenticated ||
						event.State == zk.StateHasSession {
						e.logInfo("connect", "session created in zookeeper")
					} else if event.State == zk.StateAuthFailed ||
						event.State == zk.StateDisconnected ||
						event.State == zk.StateExpired {
						e.logInfo("connect", "zookeeper connection was lost")
						e.disconnect()
						e.messageChannel <- Disconnected
						for {
							time.Sleep(time.Duration(e.config.ReconnectionTimeout) * time.Second)
							e.zkConnection, e.connectionChannel, err = zk.Connect(e.config.ZKURL, time.Duration(e.config.SessionTimeout)*time.Second)
							if err != nil {
								e.logError("connect", "error reconnecting to zookeeper: "+err.Error())
							} else {
								_, err := e.Start()
								if err != nil {
									e.logError("connect", "error starting election loop: "+err.Error())
								} else {
									break
								}
							}
						}
					}
				}
			case <-e.terminateChannel:
				e.logInfo("connect", "terminating connection channel")
				return
			}
		}
	}()

	return nil
}

// Start - starts to listen zk events
func (e *Manager) Start() (*chan int, error) {

	err := e.connect()
	if err != nil {
		e.logError("Start", "error connecting to zookeeper: "+err.Error())
		return nil, err
	}

	err = e.electForMaster()
	if err != nil {
		e.logError("Start", "error electing this node for master: "+err.Error())
		return nil, err
	}

	_, _, eventChannel, err := e.zkConnection.ExistsW(e.config.ZKElectionNodeURI)
	if err != nil {
		e.logError("Start", "error listening for zk events: "+err.Error())
		return nil, err
	}

	go func() {
		for {
			select {
			case event := <-eventChannel:
				if event.Type == zk.EventNodeDeleted {
					e.logInfo("Start", "master has quit, trying to be the new master...")
					err := e.electForMaster()
					if err != nil {
						e.logError("Start", "error trying to elect this node for master: "+err.Error())
					}
				} else if event.Type == zk.EventNodeCreated {
					e.logInfo("Start", "a new master has been elected...")
				}
			case event := <-e.messageChannel:
				if event == Disconnected {
					e.logInfo("Start", "breaking election loop...")
					e.isMaster = false
					e.electionChannel <- Disconnected
					return
				}
			}
		}
	}()

	return &e.electionChannel, nil
}

// disconnect - disconnects from the zookeeper
func (e *Manager) disconnect() {

	if e.zkConnection != nil && e.zkConnection.State() != zk.StateDisconnected {
		e.zkConnection.Close()
		time.Sleep(2 * time.Second)
		e.logInfo("Close", "ZK connection closed")
	} else {
		e.logInfo("Close", "ZK connection is already closed")
	}
}

// Terminate - end all channels and disconnects from the zookeeper
func (e *Manager) Terminate() {

	e.terminateChannel <- true
	e.disconnect()
}

// GetHostname - retrieves this node hostname from the OS
func (e *Manager) GetHostname() (string, error) {

	name, err := os.Hostname()
	if err != nil {
		e.logError("GetHostname", "could not retrive this node hostname: "+err.Error())
		return "", err
	}

	return name, nil
}

// registerAsSlave - register this node as a slave
func (e *Manager) registerAsSlave(nodeName string) error {

	data, err := e.getNodeData(e.config.ZKSlaveNodesURI)
	if err != nil {
		return err
	}

	if data == nil {
		path, err := e.zkConnection.Create(e.config.ZKSlaveNodesURI, []byte(nodeName), int32(0), e.defaultACL)
		if err != nil {
			e.logError("registerAsSlave", "error creating slave node directory: "+err.Error())
			return err
		}
		e.logInfo("registerAsSlave", "slave node directory created: "+path)
	}

	slaveNode := e.config.ZKSlaveNodesURI + "/" + nodeName

	data, err = e.getNodeData(slaveNode)
	if err != nil {
		return err
	}

	if data == nil {
		path, err := e.zkConnection.Create(slaveNode, []byte(nodeName), int32(zk.FlagEphemeral), e.defaultACL)
		if err != nil {
			e.logError("registerAsSlave", "error creating a slave node: "+err.Error())
			return err
		}

		e.logInfo("registerAsSlave", "slave node created: "+path)
	} else {
		e.logInfo("registerAsSlave", "slave node already exists: "+slaveNode)
	}

	e.isMaster = false
	e.electionChannel <- Slave

	return nil
}

// electForMaster - try to elect this node as the master
func (e *Manager) electForMaster() error {

	name, err := e.GetHostname()
	if err != nil {
		return err
	}

	zkMasterNode, err := e.getZKMasterNode()
	if err != nil {
		return err
	}

	if zkMasterNode != nil {
		if name == *zkMasterNode {
			e.logInfo("electForMaster", "this node is the master: "+*zkMasterNode)
			e.isMaster = true
		} else {
			e.logInfo("electForMaster", "another node is the master: "+*zkMasterNode)
			return e.registerAsSlave(name)
		}
	}

	path, err := e.zkConnection.Create(e.config.ZKElectionNodeURI, []byte(name), int32(zk.FlagEphemeral), e.defaultACL)
	if err != nil {
		if err.Error() == "zk: node already exists" {
			e.logInfo("electForMaster", "some node has became master before this node")
			return e.registerAsSlave(name)
		}

		e.logError("electForMaster", "error creating node: "+err.Error())
		return err
	}

	e.logInfo("electForMaster", "master node created: "+path)
	e.isMaster = true
	e.electionChannel <- Master

	slaveNode := e.config.ZKSlaveNodesURI + "/" + name
	slave, err := e.getNodeData(slaveNode)
	if err != nil {
		e.logError("electForMaster", fmt.Sprintf("error retrieving a slave node data '%s': %s\n", slaveNode, err.Error()))
		return nil
	}

	if slave != nil {
		err = e.zkConnection.Delete(slaveNode, 0)
		if err != nil {
			e.logError("electForMaster", fmt.Sprintf("error deleting slave node '%s': %s\n", slaveNode, err.Error()))
		} else {
			e.logInfo("electForMaster", "slave node deleted: "+slaveNode)
		}
	}

	return nil
}

// IsMaster - check if the cluster is the master
func (e *Manager) IsMaster() bool {
	return e.isMaster
}

// GetClusterInfo - return cluster info
func (e *Manager) GetClusterInfo() (*Cluster, error) {

	if e.zkConnection == nil {
		return nil, nil
	}

	nodes := []string{}
	masterNode, err := e.getZKMasterNode()
	if err != nil {
		return nil, err
	}

	nodes = append(nodes, *masterNode)

	slaveDir, err := e.getNodeData(e.config.ZKSlaveNodesURI)
	if err != nil {
		return nil, err
	}

	var children []string
	if slaveDir != nil {
		children, _, err = e.zkConnection.Children(e.config.ZKSlaveNodesURI)
		if err != nil {
			e.logError("GetClusterInfo", "error getting slave nodes: "+err.Error())
			return nil, err
		}

		nodes = append(nodes, children...)
	} else {
		children = []string{}
	}

	return &Cluster{
		IsMaster: e.isMaster,
		Master:   *masterNode,
		Slaves:   children,
		Nodes:    nodes,
		NumNodes: len(nodes),
	}, nil
}
