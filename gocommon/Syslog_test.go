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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"bytes"

	"github.com/stretchr/testify/assert"
)

//---------------------------------------------------------------------

func fileEquals(t *testing.T, expected string, fileName string) {
	assert := assert.New(t)

	buf, err := ioutil.ReadFile(fileName)
	assert.NoError(err)

	assert.EqualValues(expected, string(buf))
}

func fileExist(s string) bool {
	if _, err := os.Stat(s); os.IsNotExist(err) {
		return false
	}
	return true
}

func safeRemove(s string) error {
	if fileExist(s) {
		err := os.Remove(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeMessage(sde bool) (*SyslogMessage, string) {
	now := time.Now().Format(time.RFC3339)

	m := NewSyslogMessage()
	m.Facility = 2
	m.Severity = 2 // pri = 2*8 + 2 = 18
	m.Version = 1
	m.TimeStamp = now
	m.HostName = "HOST"
	m.Application = "APPLICATION"
	m.Process = "1234"
	m.MessageID = "msg1of2"
	m.AuditData = nil
	m.MetricData = nil
	m.Message = "Yow"

	expected := "<18>1 " + m.TimeStamp + " HOST APPLICATION 1234 msg1of2 - Yow"

	if sde {
		m.AuditData = &AuditElement{
			Actor:  "=actor=",
			Action: "-action-",
			Actee:  "_actee_",
		}
		m.MetricData = &MetricElement{
			Name:   "=name=",
			Value:  -3.14,
			Object: "_object_",
		}

		expected = "<18>1 " + m.TimeStamp + " HOST APPLICATION 1234 msg1of2 " +
			"[pzaudit@48851 Actor=\"=actor=\" Action=\"-action-\" Actee=\"_actee_\"] " +
			"[pzmetric@48851 Name=\"=name=\" Value=\"-3.140000\" Object=\"_object_\"] " +
			"Yow"
	}

	return m, expected
}

//---------------------------------------------------------------------

func Test01SyslogMessage(t *testing.T) {
	assert := assert.New(t)

	m, expected := makeMessage(false)

	s := m.String()
	assert.EqualValues(expected, s)

	mm, err := ParseSyslogMessage(expected)
	assert.NoError(err)

	assert.EqualValues(m, mm)
}

func Test02SyslogMessageSDE(t *testing.T) {
	assert := assert.New(t)

	m, expected := makeMessage(true)

	s := m.String()
	assert.EqualValues(expected, s)

	// TODO: this won't work under we make parser understand SDEs
	//mm, err := ParseSyslogMessage(expected)
	//assert.NoError(err)
	//assert.EqualValues(m, mm)
}

func Test03SyslogWriter(t *testing.T) {
	assert := assert.New(t)

	m, expected := makeMessage(false)

	{
		// verify error if no io.Writer given
		w := &SyslogSimpleWriter{Writer: nil}
		err := w.Write(m)
		assert.Error(err)
	}

	{
		// a simple kind of writer
		var buf bytes.Buffer
		w := &SyslogSimpleWriter{Writer: &buf}
		err := w.Write(m)
		assert.NoError(err)

		actual := buf.String()
		assert.EqualValues(expected, actual)
	}
}

func Test04SyslogFileWriter(t *testing.T) {
	var err error

	assert := assert.New(t)

	fname := "./testsyslog.txt"

	err = safeRemove(fname)
	assert.NoError(err)

	m1, expected1 := makeMessage(false)
	m2, expected2 := makeMessage(true)
	{
		w := &SyslogFileWriter{FileName: fname}
		err = w.Write(m1)
		assert.NoError(err)
		err = w.Close()
		assert.NoError(err)

		fileEquals(t, expected1+"\n", fname)
	}

	{
		w := &SyslogFileWriter{FileName: fname}
		err = w.Write(m2)
		assert.NoError(err)
		err = w.Close()
		assert.NoError(err)
		fileEquals(t, expected1+"\n"+expected2+"\n", fname)
	}

	err = safeRemove(fname)
	assert.NoError(err)
}

func Test05Syslog(t *testing.T) {
	var err error

	assert := assert.New(t)

	logfile := "./mylog.txt"

	err = safeRemove(logfile)
	assert.NoError(err)

	// this is what a developer would do
	var buf bytes.Buffer
	{
		writer := &SyslogSimpleWriter{
			Writer: &buf,
		}
		logger := &Syslog{
			Writer: writer,
		}
		logger.Warning("bonk")
		logger.Error("Bonk")
		logger.Fatal("BONK")
	}

	mssg := buf.String()

	pri := func(severity int, str string) {
		facility := 1
		host, err := os.Hostname()
		assert.NoError(err)
		assert.Contains(mssg, fmt.Sprintf("<%d>", facility*8+severity))
		assert.Contains(mssg, fmt.Sprintf(" %d ", os.Getpid()))
		assert.Contains(mssg, fmt.Sprintf(" %s ", host))
		assert.Contains(mssg, str)
	}

	pri(4, "bonk")
	pri(3, "Bonk")
	pri(2, "BONK")
}
