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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

const json_required = "required"
const json_not_required = "not_required"

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
func StructInterfaceToMap(stru interface{}) (map[string]interface{}, error) {
	str, err := StructInterfaceToString(stru)
	if err != nil {
		return map[string]interface{}{}, err
	}
	inter, err := StructStringToInterface(str)
	if err != nil {
		return map[string]interface{}{}, err
	}
	val, ok := inter.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}, errors.New("Could not convert to map[string]interface{}")
	}
	return val, nil
}

func GetVarsFromStruct(struc interface{}) (map[string]interface{}, error) {
	input, ok := struc.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Structure is not of type map[string]interface{}, currently: %T", struc)
	}
	return getVarsFromStructHelper(input, map[string]interface{}{}, []string{}), nil
}
func getVarsFromStructHelper(inputObj map[string]interface{}, res map[string]interface{}, path []string) map[string]interface{} {
	for k, v := range inputObj {
		wasMap := false
		switch v.(type) {
		case map[string]interface{}:
			wasMap = true
			path = append(path, k)
			res = getVarsFromStructHelper(v.(map[string]interface{}), res, path)
		default:
			temp := ""
			for i := 0; i < len(path); i++ {
				temp += path[i] + "."
			}
			res[fmt.Sprintf("%s%s", temp, k)] = v
		}
		if wasMap {
			path = path[:len(path)-1]
		}
	}
	return res
}

func ValueIsValidArray(value interface{}) bool {
	s := reflect.ValueOf(value)
	return s.Kind() == reflect.Array || s.Kind() == reflect.Slice
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
	return str[:whereToSplit], str[whereToSplit:]
}

func UnmarshalRequired(data []byte, in interface{}) error {
	var mapp map[string]interface{}
	err := json.Unmarshal(data, in)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &mapp)
	if err != nil {
		return err
	}
	inMap, err := StructInterfaceToMap(in)
	if err != nil {
		return err
	}
	inVars, err := GetVarsFromStruct(inMap)
	if err != nil {
		return err
	}
	mapVars, err := GetVarsFromStruct(mapp)
	if err != nil {
		return err
	}
	jsonFields := getRequiredFields(in)
	for k, _ := range jsonFields {
		_, ok := inVars[k]
		if !ok {
			return errors.New(fmt.Sprintf("Error with field \"%s\"", k))
		}
	}
	for k, _ := range inVars {
		_, ok := jsonFields[k]
		if !ok {
			return errors.New(fmt.Sprintf("Error with field \"%s\"", k))
		}
	}
	for k, v := range jsonFields {
		if v != json_required {
			continue
		}
		_, ok := mapVars[k]
		if !ok {
			return errors.New(fmt.Sprintf("Field \"%s\" was specified as required but not found", k))
		}
	}
	mapp = nil
	inMap = nil
	inVars = nil
	mapVars = nil
	jsonFields = nil
	return nil
}

func getRequiredFields(in interface{}) map[string]interface{} {
	return getRequiredFieldsHelper(in, map[string]interface{}{}, []string{})
}

func getRequiredFieldsHelper(in interface{}, res map[string]interface{}, path []string) map[string]interface{} {
	val := reflect.Indirect(reflect.ValueOf(in))
	for i := 0; i < val.NumField(); i++ {
		wasStruct := false
		k := val.Type().Field(i).Name
		v := val.Field(i).Type().Name()
		tags := val.Type().Field(i).Tag
		jsnTags := tags.Get("json")
		if strings.Contains(jsnTags, json_required) {
			v = json_required
		} else {
			v = json_not_required
		}
		parts := strings.Split(jsnTags, ",")
		for k, v := range parts {
			parts[k] = strings.Trim(v, " ")
		}
		if len(parts) > 0 {
			if parts[0] != json_required && parts[0] != json_not_required && parts[0] != "" {
				k = parts[0]
			}
		}
		switch val.Field(i).Kind() {
		case reflect.Struct:
			wasStruct = true
			path = append(path, k)
			res = getRequiredFieldsHelper((reflect.New(val.Field(i).Type()).Elem()).Interface(), res, path)
		default:
			temp := ""
			for i := 0; i < len(path); i++ {
				temp += path[i] + "."
			}
			res[fmt.Sprintf("%s%s", temp, k)] = v
		}
		if wasStruct {
			path = path[:len(path)-1]
		}
	}
	return res
}
