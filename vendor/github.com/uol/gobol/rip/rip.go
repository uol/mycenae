package rip

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/uol/logh"

	"github.com/uol/gobol"
)

var (
	logErrorAsDebug bool
	logger          *logh.ContextualLogger
)

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

func (e customError) ErrorCode() string {
	return ""
}

type Validator interface {
	Validate() gobol.Error
}

type errorJSON struct {
	Error   interface{} `json:"error,omitempty"`
	Message interface{} `json:"message,omitempty"`
}

func logError(gerr gobol.Error) *zerolog.Event {
	if logger == nil {
		return nil
	}

	var ev *zerolog.Event
	if logErrorAsDebug {
		if logh.DebugEnabled {
			ev = logger.Debug()
		}
	} else {
		if logh.ErrorEnabled {
			ev = logger.Error()
		}
	}

	if ev != nil {
		ev.Str("pkg", gerr.Package()).Str("func", gerr.Function()).Err(gerr).Msg(gerr.Message())
		return ev
	}

	return nil
}

func errBasic(pkg, function, message string, code int, e error) gobol.Error {
	if e != nil {
		return customError{
			e,
			message,
			pkg,
			function,
			code,
		}
	}
	return nil
}

func errUnmarshal(pkg, function string, e error) gobol.Error {
	return errBasic(pkg, function, "Wrong JSON format", http.StatusBadRequest, e)
}

func SetLogger(forceErrorToDebugLog bool) {
	logger = logh.CreateContextualLogger("pkg", "rip")
	logErrorAsDebug = forceErrorToDebugLog
}

func FromJSON(r *http.Request, t Validator) gobol.Error {

	if r.Header.Get("Content-Encoding") == "gzip" {

		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			return errUnmarshal("rip", "FromJSON", err)
		}
		defer reader.Close()
		dec := json.NewDecoder(reader)
		err = dec.Decode(t)
		if err != nil {
			return errUnmarshal("rip", "FromJSON", err)
		}
		r.Body.Close()
		return t.Validate()
	}

	d := json.NewDecoder(r.Body)
	err := d.Decode(t)
	if err != nil {
		return errUnmarshal("rip", "FromJSON", err)
	}
	r.Body.Close()
	return t.Validate()
}

func SuccessJSON(w http.ResponseWriter, statusCode int, payload interface{}) {

	b, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Add("Content-Type", "application/json")

	w.WriteHeader(statusCode)

	w.Write(b)
}

func Success(w http.ResponseWriter, statusCode int, payload []byte) {

	w.WriteHeader(statusCode)

	if payload != nil {
		_, err := w.Write(payload)
		if err != nil {
			w.Write([]byte(err.Error()))
		}
	}
}

func Fail(w http.ResponseWriter, gerr gobol.Error) {

	var errorMessage string
	if gerr.ErrorCode() == "" {
		errorMessage = gerr.Message()
	} else {
		errorMessage = getMessageErrorCode(gerr)
	}

	defer func() {
		if r := recover(); r != nil {

			if ev := logError(gerr); ev == nil {
				log.Println(gerr.Message())
			}

			if gerr.StatusCode() < 500 && gerr.Message() == "" {
				w.WriteHeader(gerr.StatusCode())
				return
			}

			ej := errorJSON{
				Message: errorMessage,
			}

			w.WriteHeader(gerr.StatusCode())

			e := json.NewEncoder(w)
			err := e.Encode(ej)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			}

		}
	}()

	if ev := logError(gerr); ev == nil {
		log.Println(gerr.Message())
	}

	if gerr.StatusCode() < 500 && gerr.Error() == "" && gerr.Message() == "" {
		w.WriteHeader(gerr.StatusCode())
		return
	}

	ej := errorJSON{
		Error:   gerr.Error(),
		Message: errorMessage,
	}

	w.WriteHeader(gerr.StatusCode())

	e := json.NewEncoder(w)
	err := e.Encode(ej)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func getMessageErrorCode(gerr gobol.Error) string {

	if msg, ok := mapErrorMessage[gerr.ErrorCode()]; ok {
		return msg
	}
	return gerr.Message()
}
