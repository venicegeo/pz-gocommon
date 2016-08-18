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
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

func StructStringToInterface(stru string) (interface{}, error) {
	data := []byte(stru)
	source := (*json.RawMessage)(&data)
	var res interface{}
	err := json.Unmarshal(*source, &res)
	return res, err
}
func StructInterfaceToString(stru interface{}) (string, error) {
	data, err := json.MarshalIndent(stru, " ", "   ")
	return string(data), err
}

//TODO display the whole tree in the variable name
func GetVarsFromStruct(struc interface{}) (map[string]interface{}, error) {
	input, ok := struc.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Structure is not of type map[string]interface{}")
	}
	_, res, err := getVarsFromStructHelper(input, map[string]interface{}{})
	return res, err
}
func getVarsFromStructHelper(inputObj map[string]interface{}, res map[string]interface{}) (map[string]interface{}, map[string]interface{}, error) {
	outputObj := map[string]interface{}{}
	for k, v := range inputObj {
		switch v.(type) {
		case map[string]interface{}:
			var err error
			var tree map[string]interface{}
			tree, res, err = getVarsFromStructHelper(v.(map[string]interface{}), res)
			if err != nil {
				return nil, nil, err
			}
			outputObj[k] = tree
		default:
			res[k] = v
			outputObj[k] = v
		}
	}
	return outputObj, res, nil
}

func ValueIsValidArray(value string) bool {
	openCount, closedCount := 0, 0
	for i := 0; i < len(value); i++ {
		char := CharAt(value, i)
		if char == "[" {
			openCount++
		} else if char == "]" {
			closedCount++
		}
	}
	if openCount != 1 || closedCount != 1 {
		return false
	}
	if strings.HasPrefix(value, "[") && (strings.HasSuffix(value, "]") || strings.HasSuffix(value, "],")) {
		return true
	}
	return false
}
func CharAt(str string, index int) string {
	return str[index : index+1]
}
func RemoveWhitespace(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}
func InsertString(str, insert string, index int) string {
	return str[:index] + insert + str[index:]
}
func SplitString(str string, whereToSplit int) (string, string) {
	return str[:whereToSplit], str[whereToSplit+1:]
}
