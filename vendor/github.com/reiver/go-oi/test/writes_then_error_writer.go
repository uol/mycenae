package oitest


import (
	"bytes"
	"io"
)


// WritesThenErrorWriter is useful for testing things that makes
// use of or accept an io.Writer.
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
type WritesThenErrorWriter struct {
	buffer bytes.Buffer
	err error
	numbersWritten []int
	writeNumber int
}


// NewWritesThenErrorWriter returns a new *WritesThenErrorWriter.
func NewWritesThenErrorWriter(err error, numbersWritten ...int) *WritesThenErrorWriter {

	slice := make([]int, len(numbersWritten))
	copy(slice, numbersWritten)

	writer := WritesThenErrorWriter{
		err:err,
		numbersWritten:slice,
		writeNumber:0,
	}

	return &writer
}


// Write makes it so *WritesThenErrorWriter fits the io.Writer interface.
//
// *WritesThenErrorWriter's version of Write will "short write" all but
// the last call to write, where it will return the specified error (which
// could, of course, be nil, if that was specified).
func (writer *WritesThenErrorWriter) Write(p []byte) (int, error) {

	m := writer.numbersWritten[writer.writeNumber]

	writer.buffer.Write(p[:m])

	writer.writeNumber++

	if len(writer.numbersWritten) == writer.writeNumber {
		return m, writer.err
	}

	return m, io.ErrShortWrite
}


// Returns what was written to the ShortWriter as a []byte.
func (w WritesThenErrorWriter) Bytes() []byte {
	return w.buffer.Bytes()
}


// Returns what was written to the ShortWriter as a string.
func (w WritesThenErrorWriter) String() string {
	return w.buffer.String()
}

