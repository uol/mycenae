package tserr

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
)

func New(e error, msg, pkg, function string, httpCode int) gobol.Error {
	return customError{
		e,
		msg,
		pkg,
		function,
		httpCode,
		constants.StringsEmpty,
	}
}

func NewErrorWithCode(e error, msg, pkg, function string, httpCode int, errorCode string) gobol.Error {
	return customError{
		e,
		msg,
		pkg,
		function,
		httpCode,
		errorCode,
	}
}

type customError struct {
	error
	msg       string
	pkg       string
	function  string
	httpCode  int
	errorCode string
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

func (e customError) ErrorCode() string {
	return e.errorCode
}
