#!/bin/bash

go run ./cmd/main.go
go run ../project-version/project-v4/cmd/main.go


for i in {10..50}
do
    curl -X POST -d "${i} * * * * *,get,www.baidu.com" http://localhost:8080/

done

