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
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/venicegeo/pz-gocommon/gocommon"
)

// MappingElementTypeName is just an alias for a string.
type MappingElementTypeName string

// Constants indicating ascending (1,2,3) or descending (3,2,1) order.
type SortOrder bool

const (
	SortAscending  SortOrder = false
	SortDescending SortOrder = true
)

type QueryFormat struct {
	Size  int
	From  int
	Order SortOrder
	Key   string
}

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
	Create(settings string) error
	Close() error
	Delete() error
	PostData(typ string, id string, obj interface{}) (*IndexResponse, error)
	GetByID(typ string, id string) (*GetResult, error)
	DeleteByID(typ string, id string) (*DeleteResponse, error)
	FilterByMatchAll(typ string, format QueryFormat) (*SearchResult, error)
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

func GetFormatParamsV2(queryFunc piazza.QueryFunc,
	defaultSize int, defaultFrom int, defaultKey string, defaultOrder SortOrder) (QueryFormat, error) {

	paramInt := func(param string, defalt int) (int, error) {
		str := queryFunc(param)
		if str == "" {
			return defalt, nil
		}

		value64, err := strconv.ParseInt(str, 10, 0)
		if err != nil {
			s := fmt.Sprintf("query argument for '?%s' is invalid: %s (%s)", param, str, err.Error())
			err := errors.New(s)
			return -1, err
		}
		value := int(value64)

		return value, nil
	}

	paramString := func(param string, defalt string) string {
		str := queryFunc(param)
		if str == "" {
			return defalt
		}
		return str
	}

	paramOrder := func(param string, defalt SortOrder) SortOrder {
		str := queryFunc(param)
		if str == "" {
			return defalt
		}

		//value, err := strconv.ParseBool(str)
		value := strings.ToLower(str) == "desc"

		// if err != nil {
		// 	c.String(http.StatusBadRequest, "query argument for '?%s' is invalid: %s", param, str)
		// 	return defalt
		// }

		return SortOrder(value)
	}

	size, err := paramInt("perPage", defaultSize)
	if err != nil {
		qf := QueryFormat{}
		return qf, err
	}

	pi, err := paramInt("page", defaultFrom)
	if err != nil {
		qf := QueryFormat{}
		return qf, err
	}

	format := QueryFormat{
		Size:  size,
		From:  pi * size,
		Key:   paramString("sortBy", defaultKey),
		Order: paramOrder("order", defaultOrder),
	}

	return format, nil
}

func GetFormatParams(c *gin.Context,
	defaultSize int, defaultFrom int, defaultKey string, defaultOrder SortOrder) QueryFormat {

	paramInt := func(param string, defalt int) int {
		str := c.Query(param)
		if str == "" {
			return defalt
		}

		value64, err := strconv.ParseInt(str, 10, 0)
		if err != nil {
			c.String(http.StatusBadRequest, "query argument for '?%s' is invalid: %s", param, str)
			return -1
		}
		value := int(value64)

		return value
	}

	paramString := func(param string, defalt string) string {
		str := c.Query(param)
		if str == "" {
			return defalt
		}
		return str
	}

	paramOrder := func(param string, defalt SortOrder) SortOrder {
		str := c.Query(param)
		if str == "" {
			return defalt
		}

		value, err := strconv.ParseBool(str)
		if err != nil {
			c.String(http.StatusBadRequest, "query argument for '?%s' is invalid: %s", param, str)
			return defalt
		}

		return SortOrder(value)
	}

	format := QueryFormat{
		Size:  paramInt("size", defaultSize),
		From:  paramInt("from", defaultFrom),
		Key:   paramString("key", defaultKey),
		Order: paramOrder("order", defaultOrder),
	}

	return format
}

func (format QueryFormat) String() string {
	return fmt.Sprintf("Size=%d, From=%d, Key=%s, Order=%t",
		format.Size, format.From, format.Key, format.Order)
}
