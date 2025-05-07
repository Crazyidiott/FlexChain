#!/bin/bash

INTERFACE="ens5"  # AWS上的默认网络接口
INTERVAL=1        # 监控间隔(秒)

while true; do
    rx_bytes_prev=$(cat /sys/class/net/$INTERFACE/statistics/rx_bytes)
    tx_bytes_prev=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes)
    
    sleep $INTERVAL
    
    rx_bytes_curr=$(cat /sys/class/net/$INTERFACE/statistics/rx_bytes)
    tx_bytes_curr=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes)
    
    rx_bps=$(( (rx_bytes_curr - rx_bytes_prev) * 8 / INTERVAL ))
    tx_bps=$(( (tx_bytes_curr - tx_bytes_prev) * 8 / INTERVAL ))
    
    rx_mbps=$(( rx_bps / 1000000 ))
    tx_mbps=$(( tx_bps / 1000000 ))
    
    echo "RX: $rx_mbps Mbps, TX: $tx_mbps Mbps"
    echo "RX: $rx_bps bps, TX: $tx_bps bps"
    echo "---------------------------------"

    
    sleep 1
done