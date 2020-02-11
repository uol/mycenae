package logh

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog"
)

//
// Has some useful logging functions.
// logh -> log helper
// @author rnojiri
//

// Level - type
type Level string

const (
	// INFO - log level
	INFO Level = "info"

	// DEBUG - log level
	DEBUG Level = "debug"

	// WARN - log level
	WARN Level = "warn"

	// ERROR - log level
	ERROR Level = "error"

	// FATAL - log level
	FATAL Level = "fatal"

	// PANIC - log level
	PANIC Level = "panic"

	// NONE - log level
	NONE Level = "none"

	// SILENT - log level
	SILENT Level = "silent"
)

// Format - the logger's output format
type Format string

const (
	// JSON - json format
	JSON Format = "json"

	// CONSOLE - plain text format
	CONSOLE Format = "console"
)

var (
	stdout zerolog.Logger

	// InfoEnabled - check if this level is enabled
	InfoEnabled bool

	// DebugEnabled - check if this level is enabled
	DebugEnabled bool

	// WarnEnabled - check if this level is enabled
	WarnEnabled bool

	// ErrorEnabled - check if this level is enabled
	ErrorEnabled bool

	// FatalEnabled - check if this level is enabled
	FatalEnabled bool

	// PanicEnabled - check if this level is enabled
	PanicEnabled bool
)

// ContextualLogger - a struct containing all valid event loggers (each one can be null if not enabled)
type ContextualLogger struct {
	numKeyValues int
	keyValues    []string
}

// Info - returns the event logger using the configured context
func (el *ContextualLogger) Info() *zerolog.Event {
	return el.addContext(Info())
}

// Debug - returns the event logger using the configured context
func (el *ContextualLogger) Debug() *zerolog.Event {
	return el.addContext(Debug())
}

// Warn - returns the event logger using the configured context
func (el *ContextualLogger) Warn() *zerolog.Event {
	return el.addContext(Warn())
}

// Error - returns the event logger using the configured context
func (el *ContextualLogger) Error() *zerolog.Event {
	return el.addContext(Error())
}

// Fatal - returns the event logger using the configured context
func (el *ContextualLogger) Fatal() *zerolog.Event {
	return el.addContext(Fatal())
}

// Panic - returns the event logger using the configured context
func (el *ContextualLogger) Panic() *zerolog.Event {
	return el.addContext(Panic())
}

// ConfigureGlobalLogger - configures the logger globally
func ConfigureGlobalLogger(lvl Level, fmt Format) {

	switch lvl {
	case INFO:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case DEBUG:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case WARN:
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case ERROR:
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case PANIC:
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	case FATAL:
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case NONE:
		zerolog.SetGlobalLevel(zerolog.NoLevel)
	case SILENT:
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}

	var out io.Writer

	if fmt == CONSOLE {
		out = zerolog.ConsoleWriter{Out: os.Stdout}
	} else {
		out = os.Stdout
	}

	stdout = zerolog.New(out).With().Timestamp().Logger()

	InfoEnabled = Info() != nil
	DebugEnabled = Debug() != nil
	WarnEnabled = Warn() != nil
	ErrorEnabled = Error() != nil
	PanicEnabled = Panic() != nil
	FatalEnabled = Fatal() != nil
}

// SendToStdout - logs a output with no log format
func SendToStdout(output string) {

	fmt.Println(output)
}

// Info - returns the info event logger if any
func Info() *zerolog.Event {
	if e := stdout.Info(); e.Enabled() {
		return e
	}
	return nil
}

// Debug - returns the debug event logger if any
func Debug() *zerolog.Event {
	if e := stdout.Debug(); e.Enabled() {
		return e
	}
	return nil
}

// Warn - returns the error event logger if any
func Warn() *zerolog.Event {
	if e := stdout.Warn(); e.Enabled() {
		return e
	}
	return nil
}

// Error - returns the error event logger if any
func Error() *zerolog.Event {
	if e := stdout.Error(); e.Enabled() {
		return e
	}
	return nil
}

// Panic - returns the error event logger if any
func Panic() *zerolog.Event {
	if e := stdout.Panic(); e.Enabled() {
		return e
	}
	return nil
}

// Fatal - returns the error event logger if any
func Fatal() *zerolog.Event {
	if e := stdout.Fatal(); e.Enabled() {
		return e
	}
	return nil
}

// CreateContextualLogger - creates loggers with context
func CreateContextualLogger(keyValues ...string) *ContextualLogger {

	numKeyValues := len(keyValues)
	if numKeyValues%2 != 0 {
		panic("the number of arguments must be even")
	}

	return &ContextualLogger{
		numKeyValues: numKeyValues,
		keyValues:    keyValues,
	}
}

// addContext - add event logger context
func (el *ContextualLogger) addContext(eventlLogger *zerolog.Event) *zerolog.Event {

	if eventlLogger == nil {
		return nil
	}

	for j := 0; j < el.numKeyValues; j += 2 {
		eventlLogger = eventlLogger.Str(el.keyValues[j], el.keyValues[j+1])
	}

	return eventlLogger
}
