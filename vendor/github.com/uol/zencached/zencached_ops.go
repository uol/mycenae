package zencached

import (
	"bytes"
	"fmt"
	"time"
)

//
// This file has all implemented commands from memcached.
// More information here:
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// author: rnojiri
//

// memcached commands and constants
const (
	storageFmt string = "%s %s 0 %d %d\r\n%s\r\n"
	getFmt     string = "get %s\r\n"
	deleteFmt  string = "delete %s\r\n"
	empty      string = ""
)

// memcached responses
var (
	lineBreaks []byte = []byte{[]byte("\r")[0], []byte("\n")[0]}

	// responses
	mcrValue     []byte = []byte("VALUE") // the only prefix
	mcrStored    []byte = []byte("STORED")
	mcrNotStored []byte = []byte("NOT_STORED")
	mcrEnd       []byte = []byte("END")
	mcrNotFound  []byte = []byte("NOT_FOUND")
	mcrDeleted   []byte = []byte("DELETED")

	// response set
	mcrResponseSetStored  [][]byte = [][]byte{mcrStored, mcrNotStored}
	mcrResponseSetGet     [][]byte = [][]byte{mcrValue, mcrEnd}
	mcrResponseSetDeleted [][]byte = [][]byte{mcrDeleted, mcrNotFound}
)

// memcachedCommand type
type memcachedCommand string

const (
	// Add - add some key if it not exists
	Add memcachedCommand = "add"

	// Set - sets a key if it exists or not
	Set memcachedCommand = "set"

	// get - return a key if it exists or not
	get memcachedCommand = "get"

	// delete - return a key if it exists or not
	delete memcachedCommand = "delete"
)

// countOperation - send the operation count metric
func (z *Zencached) countOperation(host string, operation memcachedCommand) {

	z.metricsCollector.Count(
		metricOperationCount,
		1,
		tagNodeName, host,
		tagOperationName, string(operation),
	)
}

// executeSend - sends a message to memcached
func (z *Zencached) executeSend(telnetConn *Telnet, operation memcachedCommand, renderedCmd string) error {

	if !z.enableMetrics {

		err := telnetConn.Send(renderedCmd)
		if err != nil {
			return err
		}

	} else {

		start := time.Now()
		err := telnetConn.Send(renderedCmd)
		if err != nil {
			return err
		}
		elapsedTime := time.Since(start)

		z.metricsCollector.Maximum(
			metricOperationTime,
			float64(elapsedTime.Milliseconds()),
			tagNodeName, telnetConn.GetHost(),
			tagOperationName, string(operation),
		)
	}

	return nil
}

// checkResponse - checks the memcached response
func (z *Zencached) checkResponse(telnetConn *Telnet, responseSet [][]byte, operation memcachedCommand, renderedCmd string) (bool, [][]byte, error) {

	response, err := telnetConn.Read(responseSet)
	if err != nil {
		return false, nil, err
	}

	if !bytes.HasPrefix(response[0], responseSet[0]) {
		if !bytes.Equal(response[0], responseSet[1]) {
			return false, nil, fmt.Errorf("memcached operation error on command:\n%s", operation)
		}

		if z.enableMetrics {
			z.metricsCollector.Count(
				metricCacheMiss,
				1,
				tagNodeName, telnetConn.GetHost(),
				tagOperationName, string(operation),
			)
		}

		return false, response, nil
	}

	if z.enableMetrics {
		z.metricsCollector.Count(
			metricCacheHit,
			1,
			tagNodeName, telnetConn.GetHost(),
			tagOperationName, string(operation),
		)
	}

	return true, response, nil
}

// Storage - performs an storage operation
func (z *Zencached) Storage(cmd memcachedCommand, routerHash []byte, key string, value string, ttl uint16) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseStorage(telnetConn, cmd, key, value, ttl)
}

// baseStorage - base storage function
func (z *Zencached) baseStorage(telnetConn *Telnet, cmd memcachedCommand, key string, value string, ttl uint16) (bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), cmd)
	}

	renderedCmd := fmt.Sprintf(storageFmt, cmd, key, ttl, len(value), value)
	err := z.executeSend(telnetConn, cmd, renderedCmd)
	if err != nil {
		return false, err
	}

	wasStored, _, err := z.checkResponse(telnetConn, mcrResponseSetStored, cmd, renderedCmd)
	if err != nil {
		return false, err
	}

	return wasStored, nil
}

// Get - performs a get operation
func (z *Zencached) Get(routerHash []byte, key string) (string, bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseGet(telnetConn, key)
}

// baseGet - the base get operation
func (z *Zencached) baseGet(telnetConn *Telnet, key string) (string, bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), get)
	}

	renderedCmd := fmt.Sprintf(getFmt, key)
	err := z.executeSend(telnetConn, get, renderedCmd)
	if err != nil {
		return empty, false, err
	}

	exists, response, err := z.checkResponse(telnetConn, mcrResponseSetGet, get, renderedCmd)
	if !exists || err != nil {
		return empty, false, err
	}

	storedValue := string(response[1])

	return storedValue, true, nil
}

// Delete - performs a delete operation
func (z *Zencached) Delete(routerHash []byte, key string) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseDelete(telnetConn, key)
}

// baseDelete - base delete operation
func (z *Zencached) baseDelete(telnetConn *Telnet, key string) (bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), delete)
	}

	renderedCmd := fmt.Sprintf(deleteFmt, key)
	err := z.executeSend(telnetConn, delete, renderedCmd)
	if err != nil {
		return false, err
	}

	exists, _, err := z.checkResponse(telnetConn, mcrResponseSetDeleted, delete, renderedCmd)
	if err != nil {
		return false, err
	}

	return exists, nil
}
