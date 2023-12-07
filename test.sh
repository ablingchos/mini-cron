#!/bin/bash

# go run ./cmd/main.go
# go run ../project-version/project-v4/cmd/main.go

start1=$1
end1=$2

start2=$2
end2=$2
# URI=("www.tencent.com" "www.baidu.com" "www.sina.com" "www.google.com" "www.bing.com")

for i in $(seq $start2 $end2)
do
    for j in $(seq $start1 $end1)
    do
        # for k in "${URI[@]}"
        # do
            # curl -X POST -d "${j} ${i} * * * *,get,${k}" http://localhost:8080/
        curl -X POST -d "${j} ${i} * * * *,get,www.bing.com" http://localhost:8080/
        # done
        sleep 1
    done
done





# for i in $(seq $start $end)
# do 
#     curl -X POST -d "* ${i} * * * *,get,www.baidu.com" http://localhost:8080/


