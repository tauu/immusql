package immusql

import (
	"context"
	"log"
	"time"
)

// LogLevel defines the logging level to be used by a logger.
type LogLevel int

// Define log levels. The default level is debug.
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
	LogLevelPanic
	LogLevelNone

	LogLevelTrace LogLevel = -1
)

// Logger is the interface for logging debug messages of the driver.
type Logger interface {
	Log(ctx context.Context, level LogLevel, msg string)
}

// defaultLogger is the default logger used if no custom logger has been set.
type defaultLogger struct {
	level LogLevel
}

func (dl defaultLogger) Log(ctx context.Context, level LogLevel, msg string) {
	// Only log the message if the level of the message is above the level of the logger.
	if level < dl.level {
		return
	}
	levelName := logLevelName(level)
	log.Printf("%v %s: %s", time.Now(), levelName, msg)
}

// logLevelName creates a printable name for a loglevel.
func logLevelName(level LogLevel) (name string) {
	switch level {
	case LogLevelDebug:
		name = "debug"
	case LogLevelInfo:
		name = "info"
	case LogLevelWarn:
		name = "warn"
	case LogLevelError:
		name = "error"
	case LogLevelFatal:
		name = "fatal"
	case LogLevelPanic:
		name = "panic"
	case LogLevelNone:
		name = "none"
	case LogLevelTrace:
		name = "trace"
	}
	return
}
