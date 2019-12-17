package files

import "time"

//
// File structs.
// @author rnojiri
//

// File - has some basic information about the file
type File struct {
	Name             string
	Path             string
	Size             int64
	LastModification time.Time
	Ignored          bool
}

// ScanResult - has all files and errors found when traversing the files in root directory
type ScanResult struct {
	Files   []*File
	Ignored []*File
	Errors  []error
}
