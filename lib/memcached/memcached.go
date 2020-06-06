package memcached

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	tlmanager "github.com/uol/timelinemanager"

	"github.com/uol/logh"
	"github.com/uol/zencached"
)

// Manages the memcached operations
// @author rnojiri

// Configuration - configuration wrapper
type Configuration struct {
	// Nodes - the memcached nodes
	Nodes []string

	// NumConnectionsPerNode - idle connection per node
	NumConnectionsPerNode int

	// ReconnectionTimeout - the time duration between connection retries
	ReconnectionTimeout string

	// MaxWriteTimeout - the max time duration to wait a write operation
	MaxWriteTimeout string

	// MaxReadTimeout - the max time duration to wait a read operation
	MaxReadTimeout string

	// MaxWriteRetries - the maximum number of write retries
	MaxWriteRetries int

	// ReadBufferSize - the size of the read buffer in bytes
	ReadBufferSize int

	// EnableMetrics - enables the memcached metrics
	EnableMetrics bool
}

var (
	// ClusterRouter - a router to signal a full cluster route
	ClusterRouter     []byte = []byte{0, 0}
	sizeClusterRouter int    = len(ClusterRouter)
)

// Memcached - main struct
type Memcached struct {
	client *zencached.Zencached
}

// New - initializes
func New(tm *tlmanager.Instance, configuration *Configuration) (*Memcached, error) {

	if configuration == nil {
		return nil, fmt.Errorf("no memcached configuration found")
	}

	if len(configuration.Nodes) == 0 {
		return nil, fmt.Errorf("no memcached nodes configured")
	}

	reconnectionTimeoutDuration, err := time.ParseDuration(configuration.ReconnectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("error parsing ReconnectionTimeout: %s", configuration.ReconnectionTimeout)
	}

	maxWriteTimeoutDuration, err := time.ParseDuration(configuration.MaxWriteTimeout)
	if err != nil {
		return nil, fmt.Errorf("error parsing MaxWriteTimeout: %s", configuration.MaxWriteTimeout)
	}

	maxReadTimeoutDuration, err := time.ParseDuration(configuration.MaxReadTimeout)
	if err != nil {
		return nil, fmt.Errorf("error parsing MaxReadTimeout: %s", configuration.MaxReadTimeout)
	}

	nodes := make([]zencached.Node, len(configuration.Nodes))
	for i := 0; i < len(configuration.Nodes); i++ {

		result := strings.Split(configuration.Nodes[i], ":")

		port, err := strconv.Atoi(result[1])
		if err != nil {
			return nil, fmt.Errorf("error parsing port: %s", result[1])
		}

		nodes[i] = zencached.Node{
			Host: result[0],
			Port: port,
		}
	}

	zConf := &zencached.Configuration{
		Nodes:                 nodes,
		NumConnectionsPerNode: configuration.NumConnectionsPerNode,
		TelnetConfiguration: zencached.TelnetConfiguration{
			MaxWriteTimeout:     maxWriteTimeoutDuration,
			MaxReadTimeout:      maxReadTimeoutDuration,
			MaxWriteRetries:     configuration.MaxWriteRetries,
			ReadBufferSize:      configuration.ReadBufferSize,
			ReconnectionTimeout: reconnectionTimeoutDuration,
		},
	}

	var zc *zencached.Zencached

	if configuration.EnableMetrics {

		if logh.InfoEnabled {
			logh.Info().Msg("memcached metrics enabled")
		}

		zc, err = zencached.New(zConf, newMetricsCollector(tm))
	} else {
		zc, err = zencached.New(zConf, nil)
	}

	if err != nil {
		return nil, err
	}

	return &Memcached{
		client: zc,
	}, nil
}

// fqn - builds a new fully qualified name using the specified strings
func (mc *Memcached) fqn(namespace []byte, fqnKeys ...string) ([]byte, error) {

	if fqnKeys == nil || len(fqnKeys) == 0 {
		return nil, fmt.Errorf("no fqn composition keys found")
	}

	keysLen := 0
	for _, key := range fqnKeys {
		keysLen += len(key) + 1
	}

	buffer := bytes.Buffer{}
	buffer.Grow(len(namespace) + 1 + keysLen)
	buffer.Write(namespace)
	buffer.WriteByte('/')
	for _, key := range fqnKeys {
		buffer.WriteString(key)
		buffer.WriteByte('/')
	}

	return buffer.Bytes(), nil
}

// Get - returns an object from the cache
func (mc *Memcached) Get(router, namespace []byte, fqnKeys ...string) ([]byte, bool, error) {

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return nil, false, err
	}

	var item []byte
	var exists bool
	if mc.isClusterRouter(router) {
		item, exists, err = mc.client.ClusterGet(fqn)
	} else {
		item, exists, err = mc.client.Get(router, fqn)
	}

	if err != nil {
		return nil, false, err
	}

	return item, exists, nil
}

// Put - puts an object in the cache
func (mc *Memcached) Put(router, value, ttl, namespace []byte, fqnKeys ...string) error {

	fqn, err := mc.fqn(namespace, fqnKeys...)
	if err != nil {
		return err
	}

	if mc.isClusterRouter(router) {
		_, errors := mc.client.ClusterStorage(zencached.Set, fqn, value, ttl)
		for i := 0; i < len(errors); i++ {
			if errors[i] != nil {
				err = errors[i]
				break
			}
		}
	} else {
		_, err = mc.client.Storage(zencached.Set, router, fqn, value, ttl)
	}
	if err != nil {
		return err
	}

	return nil
}

// Delete - deletes an object from the cache
func (mc *Memcached) Delete(router, namespace []byte, fqnKeys ...string) error {

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return err
	}

	if mc.isClusterRouter(router) {
		_, errors := mc.client.ClusterDelete(fqn)
		for i := 0; i < len(errors); i++ {
			if errors[i] != nil {
				err = errors[i]
				break
			}
		}
	} else {
		_, err = mc.client.Delete(router, fqn)
	}
	if err != nil {
		return err
	}

	return nil
}

// isClusterRouter - check if it is a cluster route
func (mc *Memcached) isClusterRouter(bytes []byte) bool {
	if len(bytes) != sizeClusterRouter {
		return false
	}
	for i := 0; i < sizeClusterRouter; i++ {
		if bytes[i] != ClusterRouter[i] {
			return false
		}
	}

	return true
}
