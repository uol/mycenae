package buffer

import (
	"sync"
	"sync/atomic"
)

/**
* Implements a safe buffer structure to add and retrieve items with no limits.
*
* @author rnojiri
**/

type node struct {
	next  *node
	value interface{}
}

// Buffer - the buffer struture
type Buffer struct {
	first    *node
	last     *node
	numItems uint64
	lock     sync.Mutex
}

// New - creates a new buffer
func New() *Buffer {

	return &Buffer{
		lock: sync.Mutex{},
	}
}

// Add - adds a new item
func (b *Buffer) Add(item interface{}) {

	b.lock.Lock()
	b.numItems++

	if b.first == nil {

		b.first = &node{
			next:  nil,
			value: item,
		}

		b.lock.Unlock()
		return
	}

	if b.last == nil {

		b.last = &node{
			next:  nil,
			value: item,
		}

		b.first.next = b.last

		b.lock.Unlock()
		return
	}

	b.last.next = &node{
		next:  nil,
		value: item,
	}

	b.last = b.last.next

	b.lock.Unlock()
}

// GetAll - return all items
func (b *Buffer) GetAll() []interface{} {

	b.lock.Lock()

	size := (int)(atomic.LoadUint64(&b.numItems))

	items := make([]interface{}, size)
	pointer := b.first

	for i := 0; i < size; i++ {

		items[i] = pointer.value
		pointer = pointer.next
	}

	b.first = nil
	b.last = nil
	b.numItems = 0

	b.lock.Unlock()

	return items
}

// Release - releases the buffer
func (b *Buffer) Release() {

	if b == nil {
		return
	}

	b.lock.Lock()

	b.first = nil
	b.last = nil
	atomic.StoreUint64(&b.numItems, 0)

	b.lock.Unlock()
}

// GetSize - returns the buffer's size
func (b *Buffer) GetSize() int {

	b.lock.Lock()

	size := atomic.LoadUint64(&b.numItems)

	b.lock.Unlock()

	return (int)(size)
}
