package constants

//
// Defines all available source types.
// @author: rnojiri
//

// SourceType - the type of the source
type SourceType struct {

	// Name - the source's name
	Name string

	// ErrorCodePrefix - the error code prefix for this error
	ErrorCodePrefix string
}

var (
	// SourceTypeHTTP - defines the source's data
	SourceTypeHTTP *SourceType = &SourceType{
		Name:            "http",
		ErrorCodePrefix: errorCodeHTTP,
	}

	// SourceTypeUDP - defines the source's data
	SourceTypeUDP *SourceType = &SourceType{
		Name:            "udp",
		ErrorCodePrefix: errorCodeUDP,
	}

	// SourceTypeTelnetNetdata - defines the source's data
	SourceTypeTelnetNetdata *SourceType = &SourceType{
		Name:            "telnet-netdata",
		ErrorCodePrefix: errorCodeTelnetNetdata,
	}

	// SourceTypeTelnetOpenTSDB - defines the source's data
	SourceTypeTelnetOpenTSDB *SourceType = &SourceType{
		Name:            "telnet-opentsdb",
		ErrorCodePrefix: errorCodeTelnetNetdata,
	}
)
