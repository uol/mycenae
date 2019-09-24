package hashing

import "reflect"

/**
* The hashing library's sort functions.
* @author rnojiri
**/

// strings

type byString []reflect.Value

func (s byString) Len() int {

	return len(s)
}

func (s byString) Swap(i, j int) {

	s[i], s[j] = s[j], s[i]
}

func (s byString) Less(i, j int) bool {

	return s[i].String() < s[j].String()
}

// floats

type byFloat []reflect.Value

func (s byFloat) Len() int {

	return len(s)
}

func (s byFloat) Swap(i, j int) {

	s[i], s[j] = s[j], s[i]
}

func (s byFloat) Less(i, j int) bool {

	return s[i].Float() < s[j].Float()
}

// int

type byInt []reflect.Value

func (s byInt) Len() int {

	return len(s)
}

func (s byInt) Swap(i, j int) {

	s[i], s[j] = s[j], s[i]
}

func (s byInt) Less(i, j int) bool {

	return s[i].Int() < s[j].Int()
}

// boolean

type byBool []reflect.Value

func (s byBool) Len() int {

	return len(s)
}

func (s byBool) Swap(i, j int) {

	s[i], s[j] = s[j], s[i]
}

func (s byBool) Less(i, j int) bool {

	var inti, intj byte

	if s[i].Bool() {
		inti = 1
	}

	if s[j].Bool() {
		intj = 1
	}
	return inti < intj
}
