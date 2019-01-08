package oitest


import (
	"bytes"
	"io"
)


// ShortWriter is useful for testing things that makes use of
// or accept an io.Writer.
//
// In particular, it is useful to see if that thing can handle
// an io.Writer that does a "short write".
//
// A "short write" is the case when a call like:
//
//	n, err := w.Write(p)
//
// Returns an n < len(p) and err == io.ErrShortWrite.
//
// A thing that can "handle" this situation might try
// writing again, but only what didn't get written.
//
// For a simple example of this:
//
//	n, err := w.Write(p)
//	
//	if io.ErrShortWrite == err {
//		n2, err2 := w.Write(p[n:])
//	}
//
// Note that the second call to the Write method passed
// 'p[n:]' (instead of just 'p'), to account for 'n' bytes
// already written (with the first call to the Write
// method).
//
// A more "production quality" version of this would likely
// be in a loop, but such that that loop had "guards" against
// looping forever, and also possibly looping for "too long".
type ShortWriter struct {
	buffer bytes.Buffer
}


// Write makes it so ShortWriter fits the io.Writer interface.
//
// ShortWriter's version of Write will "short write" if len(p) >= 2,
// else it will 
func (w *ShortWriter) Write(p []byte) (int, error) {
	if len(p) < 1 {
		return 0, nil
	}

	m := 1
	if limit := len(p)-1; limit > 1 {
		m += randomness.Intn(len(p)-1)
	}

	n, err := w.buffer.Write(p[:m])

	err = nil
	if n < len(p) {
		err = io.ErrShortWrite
	}

	return n, err
}


// Returns what was written to the ShortWriter as a []byte.
func (w ShortWriter) Bytes() []byte {
	return w.buffer.Bytes()
}


// Returns what was written to the ShortWriter as a string.
func (w ShortWriter) String() string {
	return w.buffer.String()
}
