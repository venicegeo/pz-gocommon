#!/bin/bash
set -e

pushd "$(dirname "$0")/.." > /dev/null
root=$(pwd -P)
popd > /dev/null

export GOPATH=$root/gogo
mkdir -p "$GOPATH"

# glide expects this to already exist
mkdir "$GOPATH"/bin "$GOPATH"/src "$GOPATH"/pkg

PATH=$PATH:"$GOPATH"/bin

export GO15VENDOREXPERIMENT="1"

curl https://glide.sh/get | sh

# get ourself, and go there
go get github.com/venicegeo/pz-gocommon/gocommon
cd $GOPATH/src/github.com/venicegeo/pz-gocommon

glide install

# not included in glide deps, alas
go get github.com/stretchr/testify/suite
go get github.com/stretchr/testify/assert

# run tests
go test -v -coverprofile=common.cov github.com/venicegeo/pz-gocommon/gocommon
go test -v -coverprofile=elastic.cov github.com/venicegeo/pz-gocommon/elasticsearch
go test -v -coverprofile=kafka.cov github.com/venicegeo/pz-gocommon/kafka

#go tool cover -html=common.cov
