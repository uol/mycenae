package compress

import (
	"bytes"
	"compress/gzip"
)

// UnzipBytes - unzips the specified slice of bytes
func UnzipBytes(data []byte) ([]byte, error) {

	b := bytes.NewBuffer(data)

	r, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer

	_, err = buffer.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// ZipBytes - unzips the specified slice of bytes
func ZipBytes(data []byte) ([]byte, error) {

	var buffer bytes.Buffer

	w := gzip.NewWriter(&buffer)

	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}

	if err = w.Flush(); err != nil {
		return nil, err
	}

	if err = w.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
