import re
import pandas as pd
import os
from pathlib import Path
import numpy as np

class FlexChainLogParser:
    def __init__(self, data_dir="."):
        self.data_dir = Path(data_dir)
        self.workload_types = {
            'ycsb': ['ycsb1.log', 'ycsb2.log', 'ycsb3.log'],
            'kmeans': ['kmeans1.log', 'kmeans2.log', 'kmeans3.log'], 
            'small': ['small1.log', 'small2.log', 'small3.log']
        }
        
    def parse_log_file(self, file_path):
        """解析单个日志文件，提取系统状态信息"""
        states = []
        
        with open(file_path, 'r') as f:
            content = f.read()
            
        # 使用正则表达式匹配状态信息
        pattern = r'\[State\] interval_seconds=(\d+), interval_ops: YCSB=(\d+), KMEANS=(\d+), BANK=(\d+), TOTAL=(\d+) \|\s*CPU: ([\d.]+)%, Memory: ([\d.]+)%, Cores: (\d+), Threads/Core: (\d+), EVICT_THR: (\d+)\s*cache_hit: (\d+), sst_cnt: (\d+), total_ops: (\d+)'
        
        matches = re.findall(pattern, content)
        
        for match in matches:
            state = {
                'interval_seconds': int(match[0]),
                'ycsb_ops': int(match[1]),
                'kmeans_ops': int(match[2]),
                'bank_ops': int(match[3]),
                'total_ops_interval': int(match[4]),  # 这是interval内的总操作数
                'cpu_utilization': float(match[5]),
                'memory_utilization': float(match[6]),
                'core_count': int(match[7]),
                'threads_per_core': int(match[8]),
                'evict_threshold': int(match[9]),
                'cache_hit': int(match[10]),
                'sst_count': int(match[11]),
                'total_ops_cumulative': int(match[12])  # 这是累积的总操作数
            }
            
            # 计算请求总数（interval内）
            state['request_total'] = state['ycsb_ops'] + state['kmeans_ops'] + state['bank_ops']
            
            # 计算内存利用率（根据你的公式）
            state['memory_utilization_normalized'] = self.calculate_memory_utilization(
                state['evict_threshold'], 400000  # 假设总内存地址为400000
            )
            
            states.append(state)
            
        return states
    
    def calculate_memory_utilization(self, evict_thr, total_memory=400000):
        """
        根据设计文档中的公式计算内存利用率
        memory_utilization = (total_available_address - free_addrs) / (total_available_address - evict_thr)
        """
        # 这里我们需要从日志中提取free_addrs信息，暂时用一个估算值
        # 你可能需要根据实际日志格式调整
        if evict_thr >= total_memory:
            return 1.0
        return max(0.0, min(1.0, (total_memory - evict_thr) / (total_memory - evict_thr)))
    
    def parse_all_files(self):
        """解析所有文件并返回结构化数据"""
        all_data = {}
        
        for workload_type, file_list in self.workload_types.items():
            all_data[workload_type] = {}
            
            for i, filename in enumerate(file_list, 1):
                file_path = self.data_dir / filename
                if file_path.exists():
                    print(f"解析文件: {filename}")
                    states = self.parse_log_file(file_path)
                    all_data[workload_type][f'size_{i}'] = states
                    print(f"  提取到 {len(states)} 个状态样本")
                else:
                    print(f"文件不存在: {filename}")
                    
        return all_data
    
    def export_to_csv(self, data, output_dir="processed_data"):
        """将解析的数据导出为CSV文件"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        for workload_type, sizes in data.items():
            for size_key, states in sizes.items():
                if states:  # 确保有数据
                    df = pd.DataFrame(states)
                    filename = f"{workload_type}_{size_key}.csv"
                    df.to_csv(output_path / filename, index=False)
                    print(f"导出: {filename} ({len(states)} 行)")
    
    def get_summary_statistics(self, data):
        """生成数据摘要统计"""
        summary = {}
        
        for workload_type, sizes in data.items():
            summary[workload_type] = {}
            
            for size_key, states in sizes.items():
                if states:
                    df = pd.DataFrame(states)
                    summary[workload_type][size_key] = {
                        'sample_count': len(states),
                        'core_count_range': f"{df['core_count'].min()}-{df['core_count'].max()}",
                        'cpu_utilization_avg': f"{df['cpu_utilization'].mean():.2f}%",
                        'memory_utilization_avg': f"{df['memory_utilization'].mean():.2f}%",
                        'throughput_avg': f"{df['total_ops_interval'].mean():.2f}",
                        'request_types': {
                            'ycsb_avg': df['ycsb_ops'].mean(),
                            'kmeans_avg': df['kmeans_ops'].mean(), 
                            'bank_avg': df['bank_ops'].mean()
                        }
                    }
                    
        return summary

def main():
    # 初始化解析器
    parser = FlexChainLogParser(".")
    
    # 解析所有文件
    print("开始解析日志文件...")
    data = parser.parse_all_files()
    
    # 导出为CSV
    print("\n导出CSV文件...")
    parser.export_to_csv(data)
    
    # 生成摘要统计
    print("\n生成摘要统计...")
    summary = parser.get_summary_statistics(data)
    
    # 打印摘要
    for workload_type, sizes in summary.items():
        print(f"\n=== {workload_type.upper()} WORKLOAD ===")
        for size_key, stats in sizes.items():
            print(f"\n{size_key}:")
            print(f"  样本数量: {stats['sample_count']}")
            print(f"  核心数范围: {stats['core_count_range']}")
            print(f"  平均CPU利用率: {stats['cpu_utilization_avg']}")
            print(f"  平均内存利用率: {stats['memory_utilization_avg']}")
            print(f"  平均吞吐量: {stats['throughput_avg']}")
            print(f"  请求类型分布: YCSB={stats['request_types']['ycsb_avg']:.1f}, "
                  f"KMEANS={stats['request_types']['kmeans_avg']:.1f}, "
                  f"BANK={stats['request_types']['bank_avg']:.1f}")

if __name__ == "__main__":
    main()