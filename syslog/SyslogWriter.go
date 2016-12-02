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

package syslog

import (
	"fmt"
	"io"
	"os"
)

//---------------------------------------------------------------------

// Writer is an interface for writing a Message to some sort of output.
type Writer interface {
	Write(*Message) error
}

//---------------------------------------------------------------------

// IOWriter implements the WriterI interface, writing to a generic "io.Writer" target
type IOWriter struct {
	Writer io.Writer
}

// Write writes the message to the io.Writer supplied.
func (w *IOWriter) Write(mssg *Message) error {
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

// StringWriter implements the WriterI interface, writing to an array of strings
type StringWriter struct {
	Messages []string
}

// Write writes the message to the string array
func (w *StringWriter) Write(mssg *Message) error {
	if w.Messages == nil {
		w.Messages = make([]string, 0)
	}

	s := mssg.String()
	w.Messages = append(w.Messages, s)

	return nil
}

//---------------------------------------------------------------------

// FileWriter implements the WriterI, writing to a given file
type FileWriter struct {
	FileName string
	file     *os.File
}

// Write writes the message to the supplied file.
func (w *FileWriter) Write(mssg *Message) error {
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

// Close closes the file. The creator of the FileWriter must call this.
func (w *FileWriter) Close() error {
	return w.file.Close()
}
