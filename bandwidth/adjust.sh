#!/bin/bash

INTERFACE="ens5"
DEFAULT_DOWNLOAD_RATE="1gbit"  # 默认下载带宽
DEFAULT_UPLOAD_RATE="1gbit"    # 默认上传带宽

# 清除现有的tc规则
clear_tc_rules() {
    tc qdisc del dev $INTERFACE root 2> /dev/null
    echo "Cleared all bandwidth limits"
}

# 设置带宽限制
set_bandwidth_limit() {
    local download_rate=$1
    local upload_rate=$2
    
    # 确保tc和相关模块已加载
    modprobe ifb
    ip link set dev ifb0 up 2> /dev/null || ip link add ifb0 type ifb
    ip link set dev ifb0 up
    
    # 清除现有规则
    tc qdisc del dev $INTERFACE root 2> /dev/null
    tc qdisc del dev ifb0 root 2> /dev/null
    tc qdisc del dev $INTERFACE ingress 2> /dev/null
    
    # 设置下载限制(入站流量)
    tc qdisc add dev $INTERFACE handle ffff: ingress
    tc filter add dev $INTERFACE parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0
    tc qdisc add dev ifb0 root handle 1: htb default 10
    tc class add dev ifb0 parent 1: classid 1:10 htb rate $download_rate
    
    # 设置上传限制(出站流量)
    tc qdisc add dev $INTERFACE root handle 1: htb default 10
    tc class add dev $INTERFACE parent 1: classid 1:10 htb rate $upload_rate
    
    echo "Bandwidth limits set: Download=$download_rate, Upload=$upload_rate"
}

# 函数调用
case "$1" in
    clear)
        clear_tc_rules
        ;;
    set)
        set_bandwidth_limit "${2:-$DEFAULT_DOWNLOAD_RATE}" "${3:-$DEFAULT_UPLOAD_RATE}"
        ;;
    *)
        echo "Usage: $0 {clear|set [download_rate] [upload_rate]}"
        echo "Example: $0 set 10mbit 5mbit"
        exit 1
        ;;
esac