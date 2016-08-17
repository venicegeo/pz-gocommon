package piazza

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

type ObjIdent struct {
	Index int
	Type  string
	Name  string
}
type ObjPair struct {
	OpenIndex   int
	ClosedIndex int
	Range       int
	Name        string
}

var closed, open = "closed", "open"

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

/*
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
*/
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
func PairsContain(pairs []ObjPair, index int) bool {
	for _, pair := range pairs {
		if index >= pair.OpenIndex && index <= pair.ClosedIndex {
			return true
		}
	}
	return false
}

func GetVariablesFromStructInterface(stru interface{}) ([]string, []string, error) {
	keys, values := []string{}, []string{}
	str, err := StructInterfaceToString(stru)
	if err != nil {
		return nil, nil, err
	}
	str = RemoveWhitespace(str)
	//-------------Find all open and closed quotes------------------------------
	quotes := []ObjPair{}
	qO, qC := -1, 0
	for i := 0; i < len(str); i++ {
		char := CharAt(str, i)
		if char == "\"" {
			if i != 0 {
				charBefore := CharAt(str, i-1)
				if charBefore != "\\" {
					qC++
					if qO == -1 {
						qO = i
					} else {
						quotes = append(quotes, ObjPair{qO, i, 0, ""})
						qO = -1
					}
				}
			} else {
				qC++
				if qO == -1 {
					qO = i
				}
			}
		}
	}
	if len(quotes)*2 != qC {
		return nil, nil, fmt.Errorf("Not enough quotes: %s %s*2", qC, len(quotes))
	}
	//-------------Find all open and closed brackets----------------------------
	idents := []ObjIdent{}
	oC, cC := 0, 0
	for i := 0; i < len(str); i++ {
		char := CharAt(str, i)
		if char == "{" && !PairsContain(quotes, i) {
			oC++
			idents = append(idents, ObjIdent{i, open, ""})
		} else if char == "}" && !PairsContain(quotes, i) {
			cC++
			idents = append(idents, ObjIdent{i, closed, ""})
		}
	}
	if oC != cC {
		return nil, nil, fmt.Errorf("Not correct brackets: %s != %s", oC, cC)
	}
	//-------------Match brackets into pairs------------------------------------
	pairs := []ObjPair{}
	pairMap := map[int]int{}
	for len(idents) > 0 {
		for i := 0; i < len(idents)-1; i++ {
			a := idents[i]
			b := idents[i+1]
			if a.Type == open && b.Type == closed {
				pairMap[a.Index] = b.Index
				idents = append(idents[:i], idents[i+1:]...)
				idents = append(idents[:i], idents[i+1:]...)
				break
			}
		}
	}
	//-------------Sort pairs based off open bracket index----------------------
	oKeys := []int{}
	for k, _ := range pairMap {
		oKeys = append(oKeys, k)
	}
	sort.Ints(oKeys)
	for _, k := range oKeys {
		v := pairMap[k]
		pairs = append(pairs, ObjPair{k, v, v - k, ""})
	}
	//-------------Seperate pieces of mapping onto seperate lines---------------
	temp := ""
	squareBracketOpen := false
	for i := 0; i < len(str); i++ {
		if CharAt(str, i) == "[" && !PairsContain(quotes, i) {
			squareBracketOpen = true
		} else if CharAt(str, i) == "]" && !PairsContain(quotes, i) {
			squareBracketOpen = false
		}
		if ((CharAt(str, i) == "}" || CharAt(str, i) == ",") && !squareBracketOpen) && !PairsContain(quotes, i) {
			temp += "\n" + CharAt(str, i) + "\n"
		} else if (CharAt(str, i) == "{" && !squareBracketOpen) && !PairsContain(quotes, i) {
			temp += CharAt(str, i) + "\n"
		} else {
			temp += CharAt(str, i)
		}
	}
	lines := strings.Split(temp, "\n")
	toRemove := []int{}
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" {
			toRemove = append(toRemove, i)
		}
	}
	for len(toRemove) > 0 {
		index := toRemove[0]
		toRemove = append(toRemove[:0], toRemove[0+1:]...)
		lines = append(lines[:index], lines[index+1:]...)
		for i := 0; i < len(toRemove); i++ {
			toRemove[i]--
		}
	}
	//-------------FIND STRUCTURE NAMES-----------------------------------------
	k := 0
	for _, line := range lines {
		if strings.HasPrefix(line, `"`) && strings.HasSuffix(line, `":{`) {
			i := len(line) + k - 1
			for j := 0; j < len(pairs); j++ {
				if pairs[j].OpenIndex == i {
					pairs[j].Name = (line)
				}
			}
		}
		k += len(line)
	}
	//-------------FIND VARIABLES-----------------------------------------------
	j := 0
	for _, line := range lines {
		if strings.HasPrefix(line, `"`) && !strings.HasSuffix(line, "{") {
			if strings.Contains(line, `":`) {
				toSplit := []int{}
				for i := 1; i < len(line); i++ {
					test := line[i-1 : i+1]
					if test == `":` {
						isGood := PairsContain(quotes, i+j-1)
						if isGood {
							toSplit = append(toSplit, i)
						}
					}
				}
				if len(toSplit) > 1 {
					return nil, nil, fmt.Errorf("BAD CODE")
				}
				if len(toSplit) == 1 {
					actualSplitPoint := toSplit[0] + j
					varK, varV := SplitString(line, toSplit[0])
					containedBy := []ObjPair{}
					pairMap := map[int]ObjPair{}
					for _, p := range pairs {
						if p.OpenIndex <= actualSplitPoint && p.ClosedIndex >= actualSplitPoint {
							pairMap[p.Range] = p
						}
					}
					rKeys := []int{}
					for k, _ := range pairMap {
						rKeys = append(rKeys, k)
					}
					sort.Ints(rKeys)
					for _, k := range rKeys {
						v := pairMap[k]
						containedBy = append(containedBy, v)
					}
					varK = varK[1:]
					for _, p := range containedBy {
						toAdd := p.Name
						if strings.Trim(toAdd, " ") == "" {
							continue
						}
						varK = toAdd[1:len(toAdd)-3] + "." + varK
					}
					varK = "\"" + varK
					keys = append(keys, varK)
					values = append(values, varV)
				}
			}
		}
		j += len(line)
	}
	return keys, values, nil
}
