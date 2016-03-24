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
	"fmt"
	"strings"

	"github.com/venicegeo/pz-gocommon"
)

type MappingElementTypeName string

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
