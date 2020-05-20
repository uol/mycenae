package buffer_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/timeline/buffer"
)

type Item struct {
	value int
	name  string
}

func createRandomItem() *Item {

	value := rand.Int()
	name := fmt.Sprintf("item-%d", value)

	return &Item{
		value: value,
		name:  name,
	}
}

func testAddingItems(t *testing.T, b *buffer.Buffer, numItems int) {

	expectedItems := []*Item{}

	for i := 0; i < numItems; i++ {

		item := createRandomItem()

		expectedItems = append(expectedItems, item)

		b.Add(item)
	}

	if !assert.Equalf(t, numItems, b.GetSize(), "expected the size %d", numItems) {
		return
	}

	array := b.GetAll()
	if !assert.Lenf(t, array, numItems, "expected %d items in the array", numItems) {
		return
	}

	for i := 0; i < numItems; i++ {
		assert.Equal(t, *expectedItems[i], *(array[i].(*Item)), "item must be equal")
	}
}

// TestAddOneItem - tests the first pointer
func TestAddOneItem(t *testing.T) {

	b := buffer.New()
	defer b.Release()

	testAddingItems(t, b, 1)
}

// TestTwoItems - tests the first and last pointer
func TestTwoItems(t *testing.T) {

	b := buffer.New()
	defer b.Release()

	testAddingItems(t, b, 2)
}

// TestThreeItems - tests the first, last and a middle node pointer
func TestThreeItems(t *testing.T) {

	b := buffer.New()
	defer b.Release()

	testAddingItems(t, b, 3)
}

// TestMultipleItems - tests multiple items [0, 100]
func TestMultipleItems(t *testing.T) {

	b := buffer.New()
	defer b.Release()

	testAddingItems(t, b, 10+rand.Intn(90))
}

// TestMultipleGetAll - tests multiple items [10, 100] multiple times [5, 15]
func TestMultipleGetAll(t *testing.T) {

	b := buffer.New()
	defer b.Release()

	for i := 0; i < 5+rand.Intn(10); i++ {
		testAddingItems(t, b, 10+rand.Intn(90))
	}
}
