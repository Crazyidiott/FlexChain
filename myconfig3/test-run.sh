#!/bin/bash

# Start storage server (in background)
./storage_server -a 127.0.0.1:50051 -d ../mydata/testdb &
sleep 2

# Start memory server (in background)
./memory_server -c ./myconfig/memory.config &
sleep 2

# Start orderer as leader (in background)
./orderer -l -a 127.0.0.1:50054 -c ./myconfig/consensus.config &
sleep 2

# Start compute server as validator
./compute_server -v -a 127.0.0.1:50053 -c ./myconfig/compute.config

# Wait for all background processes
wait