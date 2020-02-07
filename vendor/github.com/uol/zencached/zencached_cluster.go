package zencached

import "math/rand"

//
// Functions to distribute a key to all the cluster.
// author: rnojiri
//

// ClusterStorage - performs an full operation operation
func (z *Zencached) ClusterStorage(cmd memcachedCommand, key string, value string, ttl uint16) ([]bool, []error) {

	stored := make([]bool, z.numNodeTelnetConns)
	errors := make([]error, z.numNodeTelnetConns)

	for i := 0; i < z.numNodeTelnetConns; i++ {

		telnetConn := z.GetTelnetConnByNodeIndex(i)
		defer z.ReturnTelnetConnection(telnetConn, i)

		stored[i], errors[i] = z.baseStorage(telnetConn, cmd, key, value, ttl)
	}

	return stored, errors
}

// ClusterGet - returns a full replicated key stored in the cluster
func (z *Zencached) ClusterGet(key string) (string, bool, error) {

	index := rand.Intn(z.numNodeTelnetConns)

	telnetConn := z.GetTelnetConnByNodeIndex(index)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseGet(telnetConn, key)
}

// ClusterDelete - deletes a key from all cluster nodes
func (z *Zencached) ClusterDelete(key string) ([]bool, []error) {

	deleted := make([]bool, z.numNodeTelnetConns)
	errors := make([]error, z.numNodeTelnetConns)

	for i := 0; i < z.numNodeTelnetConns; i++ {

		telnetConn := z.GetTelnetConnByNodeIndex(i)
		defer z.ReturnTelnetConnection(telnetConn, i)

		deleted[i], errors[i] = z.baseDelete(telnetConn, key)
	}

	return deleted, errors
}
