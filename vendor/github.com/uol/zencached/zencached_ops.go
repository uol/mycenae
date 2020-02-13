package zencached

import (
	"bytes"
	"fmt"
	"strconv"
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
	lineBreaksR byte = '\r'
	lineBreaksN byte = '\n'
	whiteSpace  byte = ' '
	zero        byte = '0'
)

// memcached responses
var (
	doubleBreaks []byte = []byte{lineBreaksR, lineBreaksN}
	// responses
	mcrValue     []byte = []byte("VALUE") // the only prefix
	mcrStored    []byte = []byte("STORED")
	mcrNotStored []byte = []byte("NOT_STORED")
	mcrEnd       []byte = []byte("END")
	mcrNotFound  []byte = []byte("NOT_FOUND")
	mcrDeleted   []byte = []byte("DELETED")

	// response set
	mcrStoredResponseSet      [][]byte = [][]byte{mcrStored, mcrNotStored}
	mcrGetCheckResponseSet    [][]byte = [][]byte{mcrEnd}
	mcrGetCheckEndResponseSet [][]byte = [][]byte{mcrValue, mcrEnd}
	mcrDeletedResponseSet     [][]byte = [][]byte{mcrDeleted, mcrNotFound}
)

// memcachedCommand type
type memcachedCommand []byte

var (
	// Add - add some key if it not exists
	Add memcachedCommand = memcachedCommand("add")

	// Set - sets a key if it exists or not
	Set memcachedCommand = memcachedCommand("set")

	// get - return a key if it exists or not
	get memcachedCommand = memcachedCommand("get")

	// delete - return a key if it exists or not
	delete memcachedCommand = memcachedCommand("delete")
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
func (z *Zencached) executeSend(telnetConn *Telnet, operation memcachedCommand, renderedCmd []byte) error {

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
func (z *Zencached) checkResponse(telnetConn *Telnet, checkReadSet, checkResponseSet [][]byte, operation memcachedCommand) (bool, []byte, error) {

	response, err := telnetConn.Read(checkReadSet)
	if err != nil {
		return false, nil, err
	}

	if !bytes.HasPrefix(response, checkResponseSet[0]) {
		if !bytes.Contains(response, checkResponseSet[1]) {
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

// renderStorageCmd - like Sprintf, but in bytes
func (z *Zencached) renderStorageCmd(cmd memcachedCommand, key, value, ttl []byte) []byte {

	length := strconv.Itoa(len(value))

	buffer := bytes.Buffer{}
	buffer.Grow(len(cmd) + len(key) + len(value) + len(ttl) + len(length) + 4 + (len(doubleBreaks) * 2) + 1)
	buffer.Write(cmd)
	buffer.WriteByte(whiteSpace)
	buffer.Write(key)
	buffer.WriteByte(whiteSpace)
	buffer.WriteByte(zero)
	buffer.WriteByte(whiteSpace)
	buffer.Write(ttl)
	buffer.WriteByte(whiteSpace)
	buffer.WriteString(length)
	buffer.Write(doubleBreaks)
	buffer.Write(value)
	buffer.Write(doubleBreaks)

	return buffer.Bytes()
}

// Storage - performs an storage operation
func (z *Zencached) Storage(cmd memcachedCommand, routerHash, key, value, ttl []byte) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseStorage(telnetConn, cmd, key, value, ttl)
}

// baseStorage - base storage function
func (z *Zencached) baseStorage(telnetConn *Telnet, cmd memcachedCommand, key, value, ttl []byte) (bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), cmd)
	}

	err := z.executeSend(telnetConn, cmd, z.renderStorageCmd(cmd, key, value, ttl))
	if err != nil {
		return false, err
	}

	wasStored, _, err := z.checkResponse(telnetConn, mcrStoredResponseSet, mcrStoredResponseSet, cmd)
	if err != nil {
		return false, err
	}

	return wasStored, nil
}

// renderKeyOnlyCmd - like Sprintf, but in bytes
func (z *Zencached) renderKeyOnlyCmd(cmd memcachedCommand, key []byte) []byte {

	buffer := bytes.Buffer{}
	buffer.Grow(len(cmd) + len(key) + 1 + len(doubleBreaks))
	buffer.Write(cmd)
	buffer.WriteByte(whiteSpace)
	buffer.Write(key)
	buffer.Write(doubleBreaks)

	return buffer.Bytes()
}

// Get - performs a get operation
func (z *Zencached) Get(routerHash []byte, key []byte) ([]byte, bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseGet(telnetConn, key)
}

// baseGet - the base get operation
func (z *Zencached) baseGet(telnetConn *Telnet, key []byte) ([]byte, bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), get)
	}

	err := z.executeSend(telnetConn, get, z.renderKeyOnlyCmd(get, key))
	if err != nil {
		return nil, false, err
	}

	exists, response, err := z.checkResponse(telnetConn, mcrGetCheckResponseSet, mcrGetCheckEndResponseSet, get)
	if !exists || err != nil {
		return nil, false, err
	}

	start, end, err := z.extractValue([]byte(response))
	if err != nil {
		return nil, false, err
	}

	return response[start:end], true, nil
}

// extractValue - extracts a value from the response
func (z *Zencached) extractValue(response []byte) (start, end int, err error) {

	start = -1
	end = -1

	for i := 0; i < len(response); i++ {
		if start == -1 && response[i] == lineBreaksN {
			start = i + 1
		} else if start >= 0 && response[i] == lineBreaksR {
			end = i
			break
		}
	}

	if start == -1 {
		err = fmt.Errorf("no value found")
	}

	if end == -1 {
		end = len(response) - 1
	}

	return
}

// Delete - performs a delete operation
func (z *Zencached) Delete(routerHash []byte, key []byte) (bool, error) {

	telnetConn, index := z.GetTelnetConnection(routerHash, key)
	defer z.ReturnTelnetConnection(telnetConn, index)

	return z.baseDelete(telnetConn, key)
}

// baseDelete - base delete operation
func (z *Zencached) baseDelete(telnetConn *Telnet, key []byte) (bool, error) {

	if z.enableMetrics {
		z.countOperation(telnetConn.GetHost(), delete)
	}

	err := z.executeSend(telnetConn, delete, z.renderKeyOnlyCmd(delete, key))
	if err != nil {
		return false, err
	}

	exists, _, err := z.checkResponse(telnetConn, mcrDeletedResponseSet, mcrDeletedResponseSet, delete)
	if err != nil {
		return false, err
	}

	return exists, nil
}
