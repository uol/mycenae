package tserr

import (
	"github.com/uol/gobol"
)

func New(e error, msg, pkg, function string, httpCode int) gobol.Error {
	return customError{
		e,
		msg,
		pkg,
		function,
		httpCode,
	}
}

type customError struct {
	error
	msg      string
	pkg      string
	function string
	httpCode int
}

func (e customError) Package() string {
	return e.pkg
}

func (e customError) Function() string {
	return e.function
}

func (e customError) Message() string {
	return e.msg
}

func (e customError) StatusCode() int {
	return e.httpCode
}
