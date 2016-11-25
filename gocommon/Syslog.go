// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package piazza

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"strconv"

	"github.com/jeromer/syslogparser/rfc5424"
)

//---------------------------------------------------------------------

// SyslogMessage represents all the fields of a native RFC5424 object, plus
// our own two SDEs.
type SyslogMessage struct {
	Facility    int            `json:"facility"`
	Severity    int            `json:"severity"`
	Version     int            `json:"version"`
	TimeStamp   string         `json:"timeStamp"`
	HostName    string         `json:"hostName"`
	Application string         `json:"application"`
	Process     string         `json:"process"`
	MessageID   string         `json:"messageId"`
	AuditData   *AuditElement  `json:"auditData"`
	MetricData  *MetricElement `json:"metricData"`
	Message     string         `json:"message"`
}

const privateEnterpriseNumber = "48851" // Flaxen's PEN

// AuditElement represents an SDE for auditing (security-specific of just general).
type AuditElement struct {
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Actee  string `json:"actee"`
}

var securityAuditActions = []string{
	"create",
	"read",
	"update",
	"delete",
}

// MetricElement represents an SDE for recoridng metrics.
type MetricElement struct {
	Name   string  `json:"name"`
	Value  float64 `json:"value"`
	Object string  `json:"object"`
}

// NewSyslogMessage returns a SyslogMessage with the defaults filled in for you.
func NewSyslogMessage() *SyslogMessage {
	var err error

	host, err := os.Hostname()
	if err != nil {
		host = "-"
	}
	host += " "

	m := &SyslogMessage{
		Facility:    1,
		Severity:    6,
		Version:     1,
		TimeStamp:   time.Now().Format(time.RFC3339),
		HostName:    host,
		Application: "",
		Process:     strconv.Itoa(os.Getpid()),
		MessageID:   "",
		AuditData:   nil,
		MetricData:  nil,
		Message:     "",
	}

	return m
}

// String builds and returns the RFC5424-style textual representation of a SyslogMessage.
func (m *SyslogMessage) String() string {
	pri := m.Facility*8 + m.Severity

	timestamp := ""
	t, err := time.Parse(time.RFC3339, m.TimeStamp)
	if err != nil {
		timestamp += "-"
	} else {
		timestamp += t.Format(time.RFC3339)
	}

	host := m.HostName
	if host == "" {
		host = "-"
	}

	application := m.Application
	if application == "" {
		application = "-"
	}

	proc := m.Process
	if proc == "" {
		proc = "-"
	}

	messageID := m.MessageID
	if messageID == "" {
		messageID = "-"
	}

	header := fmt.Sprintf("<%d>%d %s %s %s %s %s",
		pri, m.Version, timestamp, host,
		application, proc, messageID)

	sdes := []string{}
	if m.AuditData != nil {
		sdes = append(sdes, m.AuditData.String())
	}
	if m.MetricData != nil {
		sdes = append(sdes, m.MetricData.String())
	}
	sde := strings.Join(sdes, " ")
	if sde == "" {
		sde = "-"
	}

	mssg := m.Message

	s := fmt.Sprintf("%s %s %s", header, sde, mssg)
	return s
}

func ParseSyslogMessage(s string) (*SyslogMessage, error) {
	m := &SyslogMessage{}

	buff := []byte(s)
	p := rfc5424.NewParser(buff)
	err := p.Parse()
	if err != nil {
		return nil, err
	}

	parts := p.Dump()
	m.Facility = parts["facility"].(int)
	m.Severity = parts["severity"].(int)
	m.Version = parts["version"].(int)
	m.TimeStamp = parts["timestamp"].(time.Time).Format(time.RFC3339)
	m.HostName = parts["hostname"].(string)
	m.Application = parts["app_name"].(string)
	m.Process = parts["proc_id"].(string)
	m.MessageID = parts["msg_id"].(string)
	m.Message = parts["message"].(string)

	//sdes := parts["structured_data"].(string)
	//log.Printf("SDES: %s", sdes)

	return m, nil
}

// IsSecurityAudit returns true iff the audit action is something we need to formally
// record as an auidtable event.
func (m *SyslogMessage) IsSecurityAudit() bool {
	if m.AuditData == nil {
		return false
	}

	for _, s := range securityAuditActions {
		if m.AuditData.Action == s {
			return true
		}
	}
	return false
}

func (m *SyslogMessage) validate() error {
	if m.Facility != 1 {
		return fmt.Errorf("Invalid Message.Facility: %d", m.Facility)
	}
	if m.Severity < 0 || m.Severity > 7 {
		return fmt.Errorf("Invalid Message.Severity: %d", m.Severity)
	}
	if m.Version != 1 {
		return fmt.Errorf("Invalid Message.Version: %d", m.Version)
	}
	_, err := time.Parse(time.RFC3339, m.TimeStamp)
	if err != nil {
		return fmt.Errorf("Invalid Message.Time value or format: %s", m.TimeStamp)
	}

	if m.HostName == "" {
		return fmt.Errorf("Message.HostnName not set")
	}

	if m.Application == "" {
		return fmt.Errorf("Message.Application not set")
	}

	if m.Process == "" {
		return fmt.Errorf("Message.Process not set")
	}

	return nil
}

func (ae *AuditElement) validate() error {
	if ae.Actor == "" {
		return fmt.Errorf("AuditElement.Actor not set")
	}
	if ae.Action == "" {
		return fmt.Errorf("AuditElement.Action not set")
	}
	if ae.Actee == "" {
		return fmt.Errorf("AuditElement.Actee not set")
	}

	// TODO: check for valid UUIDs?

	return nil
}

// String builds and returns the RFC5424-style textual representation of an Audit SDE
func (ae *AuditElement) String() string {
	s := fmt.Sprintf("[pzaudit@%s Actor=\"%s\" Action=\"%s\" Actee=\"%s\"]",
		privateEnterpriseNumber, ae.Actor, ae.Action, ae.Actee)
	return s
}

func (me *MetricElement) validate() error {
	if me.Name == "" {
		return fmt.Errorf("MetricElement.Name not set")
	}
	if me.Object == "" {
		return fmt.Errorf("MetricElement.Object not set")
	}

	// TODO: check for valid UUIDs?

	return nil
}

// String builds and returns the RFC5424-style textual representation of an Metric SDE
func (me *MetricElement) String() string {
	s := fmt.Sprintf("[pzmetric@%s Name=\"%s\" Value=\"%f\" Object=\"%s\"]",
		privateEnterpriseNumber, me.Name, me.Value, me.Object)
	return s
}

// Validate checks to see if a SyslogMessage is well-formed.
func (m *SyslogMessage) Validate() error {
	var err error

	err = m.validate()
	if err != nil {
		return err
	}

	if m.AuditData != nil {
		err = m.AuditData.validate()
		if err != nil {
			return err
		}
	}

	if m.MetricData != nil {
		err = m.MetricData.validate()
		if err != nil {
			return err
		}
	}

	return nil
}

//---------------------------------------------------------------------

// SyslogWriter is an interface for writing a SyslogMessage to some sort of output.
type SyslogWriter interface {
	Write(*SyslogMessage) error
}

// SyslogSimpleWriter implements the SyslogWriter, writing to a generic "io.Writer" target
type SyslogSimpleWriter struct {
	Writer io.Writer
}

// Write writes the message to the io.Writer supplied.
func (w *SyslogSimpleWriter) Write(mssg *SyslogMessage) error {
	if w == nil || w.Writer == nil {
		return fmt.Errorf("writer not set not set")
	}

	s := mssg.String()
	_, err := io.WriteString(w.Writer, s)
	if err != nil {
		return err
	}
	return nil
}

//---------------------------------------------------------------------

// SyslogFileWriter implements the SyslogWriter, writing to a given file
type SyslogFileWriter struct {
	FileName string
	file     *os.File
}

// Write writes the message to the supplied file.
func (w *SyslogFileWriter) Write(mssg *SyslogMessage) error {
	var err error

	if w == nil || w.FileName == "" {
		return fmt.Errorf("writer not set not set")
	}

	if w.file == nil {
		w.file, err = os.OpenFile(w.FileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
		if err != nil {
			return err
		}
	}

	s := mssg.String()
	s += "\n"

	_, err = io.WriteString(w.file, s)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the file. The creator of the SyslogFileWriter must call this.
func (w *SyslogFileWriter) Close() error {
	return w.file.Close()
}

//---------------------------------------------------------------------

// Syslog is the "helper" class that can (should) be used by services to send messages.
// In most Piazza cases, the Writer field should be set to a SyslogElkWriter.
type Syslog struct {
	Writer SyslogWriter
}

// Warning sends a log message with severity "Warning".
func (syslog *Syslog) Warning(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 4

	syslog.Writer.Write(mssg)
}

// Error sends a log message with severity "Error".
func (syslog *Syslog) Error(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 3

	syslog.Writer.Write(mssg)
}

// Fatal sends a log message with severity "Fatal".
func (syslog *Syslog) Fatal(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 2

	syslog.Writer.Write(mssg)
}
