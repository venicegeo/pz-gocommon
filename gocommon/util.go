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
const json_deny = "deny"

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

func Unmarshal(data []byte, in interface{}) error {
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
	jsonFields := getFieldRules(in)
	fmt.Println(jsonFields)
	{ //Make sure all values exist
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
	}
	for name, rules := range jsonFields {
		for _, rule := range rules {
			_, exists := mapVars[name]
			switch rule {
			case json_required:
				if !exists {
					return errors.New(fmt.Sprintf("Field \"%s\" was specified as required but not found", name))
				}
			case json_deny:
				if !exists {
					return errors.New(fmt.Sprintf("Field \"%s\" was specified as deny but was found", name))
				}
			default:
				//Unknown rule
			}
		}
	}
	mapp = nil
	inMap = nil
	inVars = nil
	mapVars = nil
	jsonFields = nil
	return nil
}

func getFieldRules(in interface{}) map[string][]string {
	return getFieldRulesHelper(in, map[string][]string{}, []string{})
}

func getFieldRulesHelper(in interface{}, res map[string][]string, path []string) map[string][]string {
	val := reflect.Indirect(reflect.ValueOf(in))
	for i := 0; i < val.NumField(); i++ {
		wasStruct := false
		k := val.Type().Field(i).Name
		v := []string{}
		tags := val.Type().Field(i).Tag
		{ //Variable name
			jsnTags := tags.Get("json")
			if parts := strings.Split(jsnTags, ","); len(parts) > 0 {
				k = parts[0]
			}
		}
		{ //Rules
			jsnTags := tags.Get("rules")
			parts := strings.Split(jsnTags, ",")
			for _, v2 := range parts {
				v = append(v, strings.Trim(v2, " "))
			}
		}

		switch val.Field(i).Kind() {
		case reflect.Struct:
			wasStruct = true
			path = append(path, k)
			res = getFieldRulesHelper((reflect.New(val.Field(i).Type()).Elem()).Interface(), res, path)
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
