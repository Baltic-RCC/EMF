#!/usr/bin/env bash
# @Author: Tanel Treuberg
# @Github: https://github.com/The-Magicians-Code
# @Description: Development Docker container build script

docker build -t emf_tag . -f base/Dockerfile
docker run --rm -i -d \
    -v $(pwd):/code \
    --name emf emf_tag