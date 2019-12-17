package files

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

//
// GZips files
// @author rnojiri
//

// GZExtension - gz extension
const GZExtension = ".gz"

// GetFileInfo - returns the file info
func GetFileInfo(filePath string) (os.FileInfo, error) {

	input, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	fileInfo, err := input.Stat()
	if err != nil {
		return nil, err
	}

	return fileInfo, nil
}

// ReadFileBytes - read all specified file bytes
func ReadFileBytes(filePath string) ([]byte, error) {

	input, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	fileInfo, err := input.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return nil, errors.New("file " + filePath + " has zero length")
	}

	fileBuffer := make([]byte, fileSize)
	_, err = input.Read(fileBuffer)
	if err != nil {
		return nil, err
	}

	return fileBuffer, nil
}

// GzipFile - gzips a file
func GzipFile(filePath, outputPath string) error {

	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	fileBuffer, err := ReadFileBytes(filePath)
	if err != nil {
		return err
	}

	array := strings.Split(filePath, "/")
	zw := gzip.NewWriter(output)
	zw.Name = array[len(array)-1]
	zw.ModTime = time.Now()

	_, err = zw.Write(fileBuffer)
	if err != nil {
		return err
	}

	err = zw.Close()
	if err != nil {
		return err
	}

	return nil
}

// GzipDecompressFile - ungzips a file
func GzipDecompressFile(filePath, outputPath string) error {

	input, err := os.Open(filePath)
	if err != nil {
		return err
	}

	zr, err := gzip.NewReader(input)
	if err != nil {
		return err
	}

	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	_, err = io.Copy(output, zr)
	if err != nil {
		return nil
	}

	err = zr.Close()
	if err != nil {
		return err
	}

	return nil
}

// DeleteFile - deletes a file
func DeleteFile(filePath string) error {

	var err = os.Remove(filePath)
	if err != nil {
		return err
	}

	return nil
}
