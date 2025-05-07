#!/bin/bash
killall storage_server memory_server orderer compute_server

# 创建目录（如果不存在）
mkdir -p ../mydata/testdb

# 启动服务
echo "Starting storage server..."
./storage_server -a 0.0.0.0:50051 -d ../mydata/testdb &
STORAGE_PID=$!
sleep 2

echo "Starting memory server..."
./memory_server -c ../myconfig/memory.config &
MEMORY_PID=$!
sleep 2

echo "Starting orderer..."
./orderer -l -a 0.0.0.0:50053 -c ../myconfig/consensus.config &
ORDERER_PID=$!
sleep 2

echo "Starting compute server..."
./compute_server -v -a 0.0.0.0:50052 -c ../myconfig/compute.config &
COMPUTE_PID=$!

echo "All services started!"
echo "Press Ctrl+C to stop all services..."

# 等待用户中断
trap "kill $STORAGE_PID $MEMORY_PID $ORDERER_PID $COMPUTE_PID; echo 'All services stopped.'; exit 0" INT
wait