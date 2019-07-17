package utils

import "sync"

// GetSyncMapSize - returns the sync map size
func GetSyncMapSize(m *sync.Map) int {

	var length int

	m.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	return length
}
