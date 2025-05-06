#!/bin/bash

# 确保Soft-RoCE已加载
echo "Ensuring Soft-RoCE is loaded..."
if ! lsmod | grep -q rdma_rxe; then
    sudo modprobe rdma_rxe
fi

# 创建Soft-RoCE设备（如果不存在）
if ! rdma link show | grep -q rxe_0; then
    echo "Creating Soft-RoCE device rxe_0 on interface ens5..."
    sudo rdma link add rxe_0 type rxe netdev ens5
fi

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