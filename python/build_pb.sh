#!/bin/bash
#
# creates the python classes for our .proto
#

#project_base="If required enter path of core-netty python (client) folder"

rm ./src/comm_pb2.py

protoc -I=../resources --python_out=./src ../resources/comm.proto 
