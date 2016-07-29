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

echo HERE
curl https://glide.sh/get > get.sh
sh -x get.sh
echo THERE
#cp "$GOPATH"/bin/glide /usr/local/go/bin
#echo WHERE

glide install

go get github.com/stretchr/testify/suite
go get github.com/stretchr/testify/assert

go get github.com/Shopify/sarama

# ourself
#go get github.com/venicegeo/pz-gocommon/gocommon

# run tests
go test -v -coverprofile=common.cov github.com/venicegeo/pz-gocommon/gocommon
go test -v -coverprofile=elastic.cov github.com/venicegeo/pz-gocommon/elasticsearch
go test -v -coverprofile=kafka.cov github.com/venicegeo/pz-gocommon/kafka

#go tool cover -html=common.cov

###
