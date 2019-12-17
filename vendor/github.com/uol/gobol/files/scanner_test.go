package files_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/files"
)

//
// Test the file scanner
// @author rnojiri
//

func checkFiles(t *testing.T, resultFiles []*files.File, expectedFiles []string) bool {

	m := map[string]bool{}

	for _, file := range resultFiles {
		m[file.Path] = true
	}

	for _, file := range expectedFiles {
		if !assert.True(t, m[file]) {
			return false
		}
	}

	return true
}

func testScan(t *testing.T, regexp string, minSize int64, expectedFiles []string, ignoredFiles []string) {

	s := files.NewScanner(regexp, minSize)

	scanPath := getScanPathRoot("")
	r, err := s.Scan(scanPath)
	assert.NoError(t, err)
	assert.Len(t, r.Errors, 0)
	assert.Len(t, r.Files, len(expectedFiles))
	assert.Len(t, r.Ignored, len(ignoredFiles))

	checkFiles(t, r.Files, expectedFiles)
	checkFiles(t, r.Ignored, ignoredFiles)
}

func TestScanAllFiles(t *testing.T) {

	root := getScanPathRoot("")

	expected := []string{
		root + "gzip/gziped-large-text.log.gz",
		root + "gzip/large-text.log",
		root + "rootfolder/subfolder/test.log",
		root + "rootfolder/subfolder/test.log.gz",
		root + "common.go",
		root + "rootfolder/empty.log",
		root + "rootfolder/small.log",
	}

	ignored := []string{}

	testScan(t, ".*", 0, expected, ignored)
}

func TestScanSingleFile(t *testing.T) {

	root := getScanPathRoot("")

	expected := []string{
		root + "common.go",
	}

	ignored := []string{
		root + "gzip/gziped-large-text.log.gz",
		root + "gzip/large-text.log",
		root + "rootfolder/subfolder/test.log",
		root + "rootfolder/subfolder/test.log.gz",
		root + "rootfolder/empty.log",
		root + "rootfolder/small.log",
	}

	testScan(t, "\\.go$", 0, expected, ignored)
}

func TestScanNoFiles(t *testing.T) {

	root := getScanPathRoot("")

	expected := []string{}

	ignored := []string{
		root + "common.go",
		root + "gzip/gziped-large-text.log.gz",
		root + "gzip/large-text.log",
		root + "rootfolder/subfolder/test.log",
		root + "rootfolder/subfolder/test.log.gz",
		root + "rootfolder/empty.log",
		root + "rootfolder/small.log",
	}

	testScan(t, "\\.exe$", 0, expected, ignored)
}

func TestScanWithMinSize(t *testing.T) {

	root := getScanPathRoot("")

	expected := []string{
		root + "gzip/gziped-large-text.log.gz",
		root + "gzip/large-text.log",
		root + "rootfolder/subfolder/test.log",
		root + "rootfolder/subfolder/test.log.gz",
		root + "common.go",
	}

	ignored := []string{
		root + "rootfolder/empty.log",
		root + "rootfolder/small.log",
	}

	testScan(t, ".*", 3, expected, ignored)
}
