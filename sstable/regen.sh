#!/bin/bash

protoc --gogo_out=. -I=.:$GOPATH/src/github.com/gogo/protobuf/protobuf:$GOPATH/src sstable.proto
