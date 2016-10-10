#!/bin/bash
set -e

pushd "$(dirname "$0")/.." > /dev/null
root=$(pwd -P)
popd > /dev/null

#----------------------------------------------------------------------

export GOPATH=$root/gogo
mkdir -p "$GOPATH"

# glide expects this to already exist
mkdir "$GOPATH"/bin "$GOPATH"/src "$GOPATH"/pkg

PATH=$PATH:"$GOPATH"/bin

go version

# install metalinter
go get -u github.com/alecthomas/gometalinter
gometalinter --install

# build ourself, and go there
go get github.com/venicegeo/pz-gocommon/gocommon
cd $GOPATH/src/github.com/venicegeo/pz-gocommon

#----------------------------------------------------------------------

# run tests
go test -v -coverprofile=common.cov github.com/venicegeo/pz-gocommon/gocommon
go test -v -coverprofile=elastic.cov github.com/venicegeo/pz-gocommon/elasticsearch
go test -v -coverprofile=kafka.cov github.com/venicegeo/pz-gocommon/kafka

sh ci/metalinter.sh | tee lint.txt
wc -l lint.txt

#curl https://glide.sh/get | sh
#go tool cover -html=common.cov
