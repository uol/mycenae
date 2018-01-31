package tools

import (
	"fmt"
	"math/rand"
	"time"
)

// CassandraSettings abstracts the configuration of a cassandra cluster
type CassandraSettings struct {
	Keyspace       string
	Consistency    string
	Nodes          []string
	Username       string
	Password       string
	Connections    int
	Retry          int
	DiscoverHosts  bool
	DiscoverySleep int
	PageSize       int
	Timeout        string
	ProtoVersion   int
}

// ElasticsearchSettings abstracts the configuration of an Elastic Search cluster
type ElasticsearchSettings struct {
	Node    string
	Port    string
	Timeout time.Duration
}

type MycenaeSettings struct {
	Node    string
	Port    string
	Timeout time.Duration
}

// Tool is the main structure to be created from this package
type Tool struct {
	Cassandra     *cassTool
	ElasticSearch *esTool
	HTTP          *httpTool
	UDP           *udpTool
	Mycenae       *mycenaeTool
}

// InitCass initializes the cassandra session
func (t *Tool) InitCass(cassSet CassandraSettings) {
	cass := new(cassTool)
	cass.init(cassSet)
	t.Cassandra = cass
}

// InitEs initializes the Elastic Search connection
func (t *Tool) InitEs(esSet ElasticsearchSettings) {
	es := new(esTool)
	es.Init(esSet)
	t.ElasticSearch = es
	return
}

// InitHTTP initializes the http toolkit
func (t *Tool) InitHTTP(hostname string, port string, timeout time.Duration) {
	ht := new(httpTool)
	ht.Init(hostname, port, timeout)
	t.HTTP = ht
	return
}

// InitUDP initializes the UDP toolkit
func (t *Tool) InitUDP(hostname string, port string) {
	u := new(udpTool)
	u.Init(hostname, port)
	t.UDP = u
	return
}

// InitMycenae initializes the Mycenae connection
func (t *Tool) InitMycenae(mSet MycenaeSettings) {
	m := new(mycenaeTool)
	m.Init(mSet)
	t.Mycenae = m
	return
}

func GenerateRandomName() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("ts_%d", rand.Int())
}
