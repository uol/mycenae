package zencached_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/zencached"
)

// TestClusterStorageCommand - tests the cluster storage command
func TestClusterStorageCommand(t *testing.T) {

	key := "cluster-storage"
	value := "cluster-value-storage"

	z := createZencached(nil)
	defer z.Shutdown()

	stored, errors := z.ClusterStorage(zencached.Set, key, value, 60)

	if !assert.Len(t, stored, numNodes, "wrong number of nodes") {
		return
	}

	for i := 0; i < numNodes; i++ {
		if !assert.NoErrorf(t, errors[i], "unexpected error on node: %d", i) {
			return
		}

		if !assert.Truef(t, stored[i], "expected storage on node: %d", i) {
			return
		}

		telnetConn := z.GetTelnetConnByNodeIndex(i)
		defer z.ReturnTelnetConnection(telnetConn, i)

		err := telnetConn.Send("get " + key + "\r\n")
		if err != nil {
			panic(err)
		}

		readedBytes, err := telnetConn.Read([][]byte{[]byte("END")})
		if err != nil {
			panic(err)
		}

		if !assert.Truef(t, strings.Contains(string(readedBytes[1]), value), "expected value to be stored on node: %d", i) {
			return
		}
	}
}

// rawSetKeyOnAllNodes - set the key and value on all nodes
func rawSetKeyOnAllNodes(z *zencached.Zencached, key, value string) {

	for i := 0; i < numNodes; i++ {

		telnetConn := z.GetTelnetConnByNodeIndex(i)
		defer z.ReturnTelnetConnection(telnetConn, i)

		rawSetKey(telnetConn, key, value)
	}
}

// TestClusterGetCommand - tests the cluster get command
func TestClusterGetCommand(t *testing.T) {

	key := "cluster-get"
	value := "cluster-value-get"

	z := createZencached(nil)
	defer z.Shutdown()

	rawSetKeyOnAllNodes(z, key, value)

	for i := 0; i < 1000; i++ {

		storedValue, stored, err := z.ClusterGet(key)

		if !assert.NoErrorf(t, err, "unexpected error on tentative: %d", i) {
			return
		}

		if !assert.Truef(t, stored, "expected value to be stored on tentative: %d", i) {
			return
		}

		if !assert.Equalf(t, value, storedValue, "expected the same value stored", i) {
			return
		}
	}
}

// TestClusterDeleteCommand - tests the cluster delete command
func TestClusterDeleteCommand(t *testing.T) {

	key := "cluster-delete"
	value := "cluster-value-delete"

	z := createZencached(nil)
	defer z.Shutdown()

	rawSetKeyOnAllNodes(z, key, value)

	stored, errors := z.ClusterDelete(key)

	if !assert.Len(t, stored, numNodes, "wrong number of nodes") {
		return
	}

	for i := 0; i < numNodes; i++ {
		if !assert.NoErrorf(t, errors[i], "unexpected error on node: %d", i) {
			return
		}

		if !assert.Truef(t, stored[i], "expected delete on node: %d", i) {
			return
		}

		telnetConn := z.GetTelnetConnByNodeIndex(i)
		defer z.ReturnTelnetConnection(telnetConn, i)

		err := telnetConn.Send("get " + key + "\r\n")
		if err != nil {
			panic(err)
		}

		readedBytes, err := telnetConn.Read([][]byte{[]byte("END")})
		if err != nil {
			panic(err)
		}

		if !assert.Equalf(t, "END", string(readedBytes[0]), "found a value stored on node: %d", i) {
			return
		}
	}
}
