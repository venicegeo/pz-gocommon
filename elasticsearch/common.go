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

	"github.com/venicegeo/pz-gocommon"
)

// MappingElementTypeName is just an alias for a string.
type MappingElementTypeName string

// Constants representing the supported data types for the Event parameters.
const (
	MappingElementTypeString  MappingElementTypeName = "string"
	MappingElementTypeBool    MappingElementTypeName = "boolean"
	MappingElementTypeInteger MappingElementTypeName = "integer"
	MappingElementTypeDouble  MappingElementTypeName = "double"
	MappingElementTypeDate    MappingElementTypeName = "date"
	MappingElementTypeFloat   MappingElementTypeName = "float"
	MappingElementTypeByte    MappingElementTypeName = "byte"
	MappingElementTypeShort   MappingElementTypeName = "short"
	MappingElementTypeLong    MappingElementTypeName = "long"
)

type IIndex interface {
	GetVersion() string

	IndexName() string
	IndexExists() bool
	TypeExists(typ string) bool
	ItemExists(typ string, id string) bool
	Create() error
	Close() error
	Delete() error
	Flush() error
	PostData(typ string, id string, obj interface{}) (*IndexResponse, error)
	GetByID(typ string, id string) (*GetResult, error)
	DeleteByID(typ string, id string) (*DeleteResponse, error)
	FilterByMatchAll(typ string) (*SearchResult, error)
	FilterByTermQuery(typ string, name string, value interface{}) (*SearchResult, error)
	SearchByJSON(typ string, jsn string) (*SearchResult, error)
	SetMapping(typename string, jsn piazza.JsonString) error
	GetTypes() ([]string, error)
	GetMapping(typ string) (interface{}, error)
	AddPercolationQuery(id string, query piazza.JsonString) (*IndexResponse, error)
	DeletePercolationQuery(id string) (*DeleteResponse, error)
	AddPercolationDocument(typ string, doc interface{}) (*PercolateResponse, error)
}

func NewIndexInterface(sys *piazza.SystemConfig, index string, mocking bool) (IIndex, error) {
	var esi IIndex
	var err error

	if mocking {
		esi = NewMockIndex(index)
		return esi, nil
	}

	esi, err = NewIndex(sys, index)
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