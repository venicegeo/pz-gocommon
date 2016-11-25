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

//---------------------------------------------------------------------

// Syslogger is the "helper" class that can (should) be used by services to send messages.
// In most Piazza cases, the Writer field should be set to a SyslogElkWriter.
type Syslogger struct {
	Writer SyslogWriterI
}

// Warning sends a log message with severity "Warning".
func (syslog *Syslogger) Warning(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 4

	syslog.Writer.Write(mssg)
}

// Error sends a log message with severity "Error".
func (syslog *Syslogger) Error(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 3

	syslog.Writer.Write(mssg)
}

// Fatal sends a log message with severity "Fatal".
func (syslog *Syslogger) Fatal(text string) {
	mssg := NewSyslogMessage()
	mssg.Message = text
	mssg.Severity = 2

	syslog.Writer.Write(mssg)
}
