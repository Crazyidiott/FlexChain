#!/bin/bash

# 检查Soft-RoCE设备
echo "Checking RDMA devices:"
ibv_devices

# 检查Soft-RoCE链接
echo "Checking RDMA links:"
rdma link show

# 检查IPoIB网络接口
echo "Checking network interfaces:"
ip -o addr show | grep -i "infini"

# 测试RoCE连接
echo "Testing RoCE connectivity:"
# 假设我们有两个节点node1和node2
ibv_rc_pingpong -d rxe_0 -g 0 (在一个节点上)
ibv_rc_pingpong -d rxe_0 -g 0 127.0.0.1 (在另一个节点上)

echo "Test complete!"