package buffer

import (
	"sync"
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
	numItems int
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

	items := make([]interface{}, b.numItems)
	pointer := b.first

	for i := 0; i < b.numItems; i++ {

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

	b.first = nil
	b.last = nil
	b.numItems = 0
}

// GetSize - returns the buffer's size
func (b *Buffer) GetSize() int {

	return b.numItems
}
