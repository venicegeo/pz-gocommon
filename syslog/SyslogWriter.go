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

// Reader is an interface for reading Messages from some sort of input.
// count is the number of messages to read: 1 means the latest message,
// 2 means the two latest messages, etc. The newest message is at the end
// of the array.
type Reader interface {
	Read(count int) ([]*Message, error)
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
	return err
}

// Close closes the file. The creator of the FileWriter must call this.
func (w *FileWriter) Close() error {
	return w.file.Close()
}

//---------------------------------------------------------------------

// MessageWriter implements Reader and Writer, using an array of Messages
// as the backing store
type MessageWriter struct {
	messages []*Message
}

// Write writes the message to the backing array
func (w *MessageWriter) Write(mssg *Message) error {

	if w.messages == nil {
		w.messages = make([]*Message, 0)
	}

	w.messages = append(w.messages, mssg)

	return nil
}

// Read reads messages from the backing array. Will only return as many as are
// available; asking for too many is not an error.
func (w *MessageWriter) Read(count int) ([]*Message, error) {

	if count < 0 {
		return nil, fmt.Errorf("invalid count: %d", count)
	}

	if w.messages == nil || count == 0 {
		return make([]*Message, 0), nil
	}

	if count > len(w.messages) {
		count = len(w.messages)
	}

	n := len(w.messages)
	a := w.messages[n-count : n]

	return a, nil
}
