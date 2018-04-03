package rip

import (
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"

	"github.com/uol/gobol"

	"go.uber.org/zap"
)

var logger *zap.Logger

type customError struct {
	error
	msg      string
	httpCode int
	lf       map[string]interface{}
}

func (e customError) Message() string {
	return e.msg
}

func (e customError) StatusCode() int {
	return e.httpCode
}

func (e customError) LogFields() map[string]interface{} {
	return e.lf
}

type Validator interface {
	Validate() gobol.Error
}

type errorJSON struct {
	Error   interface{} `json:"error,omitempty"`
	Message interface{} `json:"message,omitempty"`
}

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return customError{
			e,
			s,
			code,
			map[string]interface{}{
				"package": "rest",
				"func":    f,
			},
		}
	}
	return nil
}

func errUnmarshal(f string, e error) gobol.Error {
	return errBasic(f, "Wrong JSON format", http.StatusBadRequest, e)
}

func SetLogger(l *zap.Logger) {
	logger = l
}

func FromJSON(r *http.Request, t Validator) gobol.Error {

	if r.Header.Get("Content-Encoding") == "gzip" {

		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			return errUnmarshal("", err)
		}
		defer reader.Close()
		dec := json.NewDecoder(reader)
		err = dec.Decode(t)
		if err != nil {
			return errUnmarshal("", err)
		}
		r.Body.Close()
		return t.Validate()
	}

	d := json.NewDecoder(r.Body)
	err := d.Decode(t)
	if err != nil {
		return errUnmarshal("", err)
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
	defer func() {
		if r := recover(); r != nil {

			if logger != nil {
				logger.Sugar().Error(gerr.Message(), gerr.LogFields())
			} else {
				log.Println(gerr.Message())
			}

			if gerr.StatusCode() < 500 && gerr.Message() == "" {
				w.WriteHeader(gerr.StatusCode())
				return
			}

			ej := errorJSON{
				Message: gerr.Message(),
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

	if logger != nil {
		logger.Sugar().Error(gerr.Error(), gerr.LogFields())
	} else {
		log.Println(gerr.Error())
	}

	if gerr.StatusCode() < 500 && gerr.Error() == "" && gerr.Message() == "" {
		w.WriteHeader(gerr.StatusCode())
		return
	}

	ej := errorJSON{
		Error:   gerr.Error(),
		Message: gerr.Message(),
	}

	w.WriteHeader(gerr.StatusCode())

	e := json.NewEncoder(w)
	err := e.Encode(ej)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}
