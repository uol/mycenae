package election

import "github.com/samuel/go-zookeeper/zk"

// Master - signals for master role acquisition
const Master = 1

// Slave - signals for slave role acquisition
const Slave = 2

// ClusterChanged - signals for cluster change
const ClusterChanged = 3

// Disconnected - int signal for disconnection
const Disconnected = 4

// Config - configures the election
type Config struct {
	ZKURL                  []string
	ZKElectionNodeURI      string
	ZKSlaveNodesURI        string
	ReconnectionTimeout    int
	SessionTimeout         int
	ClusterChangeCheckTime int
}

// Cluster - has cluster info
type Cluster struct {
	IsMaster bool
	Master   string
	Slaves   []string
	Nodes    []string
	NumNodes int
}

const (
	// EventDisconnected - specifies a custom event for disconnection
	EventDisconnected zk.EventType = 99
)
