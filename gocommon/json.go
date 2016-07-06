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
	"bytes"
	"encoding/json"
)

type JsonString string

// converts an arbitrary object to a real json string
func ConvertObjectToJsonString(obj interface{}, compact bool) (JsonString, error) {
	var byts []byte
	var err error

	byts, err = json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return "", err
	}

	jsn := JsonString(byts)

	if compact {
		jsn, err = jsn.ToCompactJson()
		if err != nil {
			return "", err
		}
	}

	return jsn, nil
}

// removes excess whitespace
func (input JsonString) ToCompactJson() (JsonString, error) {
	dst := new(bytes.Buffer)
	err := json.Compact(dst, []byte(input))
	if err != nil {
		return "", err
	}
	return JsonString(dst.String()), nil
}
