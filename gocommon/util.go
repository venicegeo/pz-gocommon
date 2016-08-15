package piazza

import (
	"encoding/json"
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
func GetVariablesFromStructInterface(stru interface{}) ([]string, []string, error) {
	str, err := StructInterfaceToString(stru)
	if err != nil {
		return nil, nil, err
	}
	str = RemoveWhitespace(str)
	temp := ""
	bracketOpen := false
	for i := 0; i < len(str); i++ {
		if CharAt(str, i) == "[" {
			bracketOpen = true
		} else if CharAt(str, i) == "]" {
			bracketOpen = false
		}
		if CharAt(str, i) == "{" {
			temp += CharAt(str, i) + "\n"
		} else if CharAt(str, i) == "}" || (CharAt(str, i) == "," && !bracketOpen) {
			temp += "\n" + CharAt(str, i) + "\n"
		} else {
			temp += CharAt(str, i)
		}
	}
	lines := strings.Split(temp, "\n")
	keys := []string{}
	values := []string{}
	for _, line := range lines {
		if strings.Contains(line, `":`) && !strings.Contains(line, `":{`) {
			parts := strings.Split(line, `":`)
			parts[0] = parts[0][1:]
			if strings.HasSuffix(parts[1], ",") {
				parts[1] = parts[1][:len(parts[1])-1]
			}
			keys = append(keys, parts[0])
			values = append(values, parts[1])
		}
	}
	return keys, values, nil
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
