#!/bin/bash

# go run ./cmd/main.go
# go run ../project-version/project-v4/cmd/main.go

start=$1
end=$2

for i in $(seq $start $end)
do
    curl -X POST -d "${i} * * * * *,get,www.baidu.com" http://localhost:8080/
done