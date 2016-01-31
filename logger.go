package piazza

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// LogMessage represents the contents of a messge for the logger service.
// All fields are required.
type LogMessage struct {
	Service  string `json:"service"`
	Address  string `json:"address"`
	Time     string `json:"time"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type Logger interface {
	// low-level interfaces
	PostToMessages(*LogMessage) error
	GetFromMessages() ([]LogMessage, error)
	GetFromAdminStats() (*LoggerAdminStats, error)
	GetFromAdminSettings() (*LoggerAdminSettings, error)
	PostToAdminSettings(*LoggerAdminSettings) error
}

type LoggerAdminStats struct {
	StartTime   time.Time `json:"starttime"`
	NumMessages int       `json:"num_messages"`
}

type LoggerAdminSettings struct {
	Debug bool `json:"debug"`
}

type MockLogger struct{}

func (*MockLogger) PostToMessages(m *LogMessage) error {
	log.Printf("MOCKLOG: %v", m)
	return nil
}

func (*MockLogger) GetFromMessages() ([]LogMessage, error) {
	return nil, nil
}

func (*MockLogger) GetFromAdminStats() (*LoggerAdminStats, error) {
	return &LoggerAdminStats{}, nil
}

func (*MockLogger) GetFromAdminSettings() (*LoggerAdminSettings, error) {
	return &LoggerAdminSettings{}, nil
}

func (*MockLogger) PostToAdminSettings(*LoggerAdminSettings) error {
	return nil
}

// ToString returns a LogMessage as a formatted string.
func (mssg *LogMessage) ToString() string {
	s := fmt.Sprintf("[%s, %s, %s, %s, %s]",
		mssg.Service, mssg.Address, mssg.Time, mssg.Severity, mssg.Message)
	return s
}

// SeverityDebug is for log messages that are only used in development.
const SeverityDebug = "Debug"

// SeverityInfo is for log messages that are only informative, no action needed.
const SeverityInfo = "Info"

// SeverityWarning is for log messages that indicate possible problems. Execution continues normally.
const SeverityWarning = "Warning"

// SeverityError is for log messages that indicate something went wrong. The problem is usually handled and execution continues.
const SeverityError = "Error"

// SeverityFatal is for log messages that indicate an internal error and the system is likely now unstable. These should never happen.
const SeverityFatal = "Fatal"

// Validate checks to make sure a LogMessage is properly filled out. If not, a non-nil error is returned.
func (mssg *LogMessage) Validate() error {
	if mssg.Service == "" {
		return errors.New("required field 'service' not set")
	}
	if mssg.Address == "" {
		return errors.New("required field 'address' not set")
	}
	if mssg.Time == "" {
		return errors.New("required field 'time' not set")
	}
	if mssg.Severity == "" {
		return errors.New("required field 'severity' not set")
	}
	if mssg.Message == "" {
		return errors.New("required field 'message' not set")
	}

	ok := false
	for _, code := range [...]string{SeverityDebug, SeverityInfo, SeverityWarning, SeverityError, SeverityFatal} {
		if mssg.Severity == code {
			ok = true
			break
		}
	}
	if !ok {
		return errors.New("invalid 'severity' setting")
	}

	return nil
}
