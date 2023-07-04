#!/usr/bin/env bash
docker build -t emf_tag . -f Dockerfile
docker run --rm -i -d -v $(pwd):/code --name emf emf_tag