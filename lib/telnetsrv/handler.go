package telnetsrv

// TelnetDataHandler - handles the data from the telnet interface
type TelnetDataHandler interface {

	// Handle - handles the data and send
	Handle(line string)
}
