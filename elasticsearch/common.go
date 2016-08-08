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

package elasticsearch

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/venicegeo/pz-gocommon/gocommon"
)

// MappingElementTypeName is just an alias for a string.
type MappingElementTypeName string

type QueryFormat struct {
	Size  int
	From  int
	Order bool
	Key   string
}

// Constants representing the supported data types for the Event parameters.
const (
	MappingElementTypeString      MappingElementTypeName = "string"
	MappingElementTypeLong        MappingElementTypeName = "long"
	MappingElementTypeInteger     MappingElementTypeName = "integer"
	MappingElementTypeShort       MappingElementTypeName = "short"
	MappingElementTypeByte        MappingElementTypeName = "byte"
	MappingElementTypeDouble      MappingElementTypeName = "double"
	MappingElementTypeFloat       MappingElementTypeName = "float"
	MappingElementTypeDate        MappingElementTypeName = "date"
	MappingElementTypeBool        MappingElementTypeName = "boolean"
	MappingElementTypeBinary      MappingElementTypeName = "binary"
	MappingElementTypeGeoPoint    MappingElementTypeName = "geo_point"
	MappingElementTypeGeoShape    MappingElementTypeName = "geo_shape"
	MappingElementTypeIp          MappingElementTypeName = "ip"
	MappingElementTypeCompletion  MappingElementTypeName = "completion"
	MappingElementTypeStringA     MappingElementTypeName = "[string]"
	MappingElementTypeLongA       MappingElementTypeName = "[long]"
	MappingElementTypeIntegerA    MappingElementTypeName = "[integer]"
	MappingElementTypeShortA      MappingElementTypeName = "[short]"
	MappingElementTypeByteA       MappingElementTypeName = "[byte]"
	MappingElementTypeDoubleA     MappingElementTypeName = "[double]"
	MappingElementTypeFloatA      MappingElementTypeName = "[float]"
	MappingElementTypeDateA       MappingElementTypeName = "[date]"
	MappingElementTypeBoolA       MappingElementTypeName = "[boolean]"
	MappingElementTypeBinaryA     MappingElementTypeName = "[binary]"
	MappingElementTypeGeoPointA   MappingElementTypeName = "[geo_point]"
	MappingElementTypeGeoShapeA   MappingElementTypeName = "[geo_shape]"
	MappingElementTypeIpA         MappingElementTypeName = "[ip]"
	MappingElementTypeCompletionA MappingElementTypeName = "[completion]"
)

// IIndex is an interface to Elasticsearch Index methods
type IIndex interface {
	GetVersion() string

	IndexName() string
	IndexExists() bool
	TypeExists(typ string) bool
	ItemExists(typ string, id string) bool
	Create(settings string) error
	Close() error
	Delete() error
	PostData(typ string, id string, obj interface{}) (*IndexResponse, error)
	GetByID(typ string, id string) (*GetResult, error)
	DeleteByID(typ string, id string) (*DeleteResponse, error)
	FilterByMatchAll(typ string, format *piazza.JsonPagination) (*SearchResult, error)
	GetAllElements(typ string) (*SearchResult, error)
	FilterByTermQuery(typ string, name string, value interface{}) (*SearchResult, error)
	FilterByMatchQuery(typ string, name string, value interface{}) (*SearchResult, error)
	SearchByJSON(typ string, jsn string) (*SearchResult, error)
	SetMapping(typename string, jsn piazza.JsonString) error
	GetTypes() ([]string, error)
	GetMapping(typ string) (interface{}, error)
	AddPercolationQuery(id string, query piazza.JsonString) (*IndexResponse, error)
	DeletePercolationQuery(id string) (*DeleteResponse, error)
	AddPercolationDocument(typ string, doc interface{}) (*PercolateResponse, error)
}

// NewIndexInterface constructs an IIndex
func NewIndexInterface(sys *piazza.SystemConfig, index string, settings string, mocking bool) (IIndex, error) {
	var esi IIndex
	var err error

	if mocking {
		esi = NewMockIndex(index)
		return esi, nil
	}

	esi, err = NewIndex(sys, index, settings)
	if err != nil {
		return nil, err
	}

	if esi == nil {
		return nil, errors.New("Index creation failed: returned nil")
	}

	return esi, nil
}

// ConstructMappingSchema takes a map of parameter names to datatypes and
// returns the corresponding ES DSL for it.
func ConstructMappingSchema(name string, items map[string]MappingElementTypeName) (piazza.JsonString, error) {

	const template string = `{
		"%s":{
			"properties":{
				%s
			}
		}
	}`

	stuff := make([]string, len(items))
	i := 0
	for k, v := range items {
		stuff[i] = fmt.Sprintf(`"%s": {"type":"%s"}`, k, v)
		i++
	}

	json := fmt.Sprintf(template, name, strings.Join(stuff, ","))

	return piazza.JsonString(json), nil
}

// NewQueryFormat constructs a QueryFormat
func NewQueryFormat(params *piazza.JsonPagination) *QueryFormat {

	format := &QueryFormat{
		Size:  params.PerPage,
		From:  params.Page * params.PerPage,
		Key:   params.SortBy,
		Order: params.Order == piazza.SortOrderAscending,
	}

	return format
}

type GetData func() (bool, error)

func PollFunction(fn GetData) (bool, error) {
	timeout := time.After(5 * time.Second)
	tick := time.Tick(250 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return false, errors.New("timeout reached")
		case <-tick:
			ok, err := fn()
			if err != nil {
				return false, err
			} else if ok {
				return true, nil
			}
		}
	}
}

func IsValidMappingType(mappingValue string) bool {
	if string(MappingElementTypeString) == mappingValue ||
		string(MappingElementTypeLong) == mappingValue ||
		string(MappingElementTypeInteger) == mappingValue ||
		string(MappingElementTypeShort) == mappingValue ||
		string(MappingElementTypeByte) == mappingValue ||
		string(MappingElementTypeDouble) == mappingValue ||
		string(MappingElementTypeFloat) == mappingValue ||
		string(MappingElementTypeDate) == mappingValue ||
		string(MappingElementTypeBool) == mappingValue ||
		string(MappingElementTypeBinary) == mappingValue ||
		string(MappingElementTypeGeoPoint) == mappingValue ||
		string(MappingElementTypeGeoShape) == mappingValue ||
		string(MappingElementTypeIp) == mappingValue ||
		string(MappingElementTypeCompletion) == mappingValue ||
		string(MappingElementTypeStringA) == mappingValue ||
		string(MappingElementTypeLongA) == mappingValue ||
		string(MappingElementTypeIntegerA) == mappingValue ||
		string(MappingElementTypeShortA) == mappingValue ||
		string(MappingElementTypeByteA) == mappingValue ||
		string(MappingElementTypeDoubleA) == mappingValue ||
		string(MappingElementTypeFloatA) == mappingValue ||
		string(MappingElementTypeDateA) == mappingValue ||
		string(MappingElementTypeBoolA) == mappingValue ||
		string(MappingElementTypeBinaryA) == mappingValue ||
		string(MappingElementTypeGeoPointA) == mappingValue ||
		string(MappingElementTypeGeoShapeA) == mappingValue ||
		string(MappingElementTypeIpA) == mappingValue ||
		string(MappingElementTypeCompletionA) == mappingValue {
		return true
	}
	return false
}
func IsValidArrayTypeMapping(mappingValue string) bool {
	if string(MappingElementTypeStringA) == mappingValue ||
		string(MappingElementTypeLongA) == mappingValue ||
		string(MappingElementTypeIntegerA) == mappingValue ||
		string(MappingElementTypeShortA) == mappingValue ||
		string(MappingElementTypeByteA) == mappingValue ||
		string(MappingElementTypeDoubleA) == mappingValue ||
		string(MappingElementTypeFloatA) == mappingValue ||
		string(MappingElementTypeDateA) == mappingValue ||
		string(MappingElementTypeBoolA) == mappingValue ||
		string(MappingElementTypeBinaryA) == mappingValue ||
		string(MappingElementTypeGeoPointA) == mappingValue ||
		string(MappingElementTypeGeoShapeA) == mappingValue ||
		string(MappingElementTypeIpA) == mappingValue ||
		string(MappingElementTypeCompletionA) == mappingValue {
		return true
	}
	return false
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
