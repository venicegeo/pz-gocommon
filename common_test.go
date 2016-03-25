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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CommonTester struct {
	suite.Suite
	sys *SystemConfig
}

func TestRunSuite(t *testing.T) {
	s := &CommonTester{}
	suite.Run(t, s)
}

func (suite *CommonTester) SetupSuite() {
	//t := suite.T()
}

func (suite *CommonTester) TearDownSuite() {
}

func (suite *CommonTester) TestNop() {
	t := suite.T()
	assert := assert.New(t)

	assert.True(!false)
}

func (suite *CommonTester) TestSystemConfig() {
	t := suite.T()
	assert := assert.New(t)

	endpoints := &ServicesMap{}

	_, err := NewSystemConfig(PzTest, endpoints)
	assert.NoError(err)
}
