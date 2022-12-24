#!/bin/bash
docker build -f docker/Dockerfile.ubuntu -t arcadia-ubuntu . && docker run arcadia-ubuntu