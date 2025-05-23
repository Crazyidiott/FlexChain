# plot_performance.py
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os
import glob

def plot_performance_data(csv_file, output_dir='plots'):
    """从CSV文件中读取性能数据并绘制图表"""
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取CSV数据
    df = pd.read_csv(csv_file)
    
    # 设置时间作为索引（如果需要）
    # df['timestamp'] = pd.to_datetime(df['timestamp'])
    # df.set_index('timestamp', inplace=True)
    
    # 创建图表
    plt.figure(figsize=(12, 8))
    
    # 绘制请求总数和完成任务数
    plt.plot(df['elapsed_seconds'], df['request_total'], label='request total', marker='o', markersize=3, linestyle='-')
    plt.plot(df['elapsed_seconds'], df['total_ops'], label='tasks completed', marker='x', markersize=3, linestyle='-')
    
    # 添加标题和标签
    plt.title('performance')
    plt.xlabel('time(s)')
    plt.ylabel('operations')
    plt.legend()
    plt.grid(True)
    
    # 保存图表
    base_name = os.path.basename(csv_file).replace('.csv', '')
    output_file = os.path.join(output_dir, f'{base_name}_plot.png')
    plt.savefig(output_file, dpi=300)
    print(f"图表已保存: {output_file}")
    
    # 显示图表
    plt.show()
    
    # 绘制更多图表：CPU和内存利用率
    plt.figure(figsize=(12, 8))
    plt.plot(df['elapsed_seconds'], df['cpu_utilization'], label='CPU utiliazation', color='red')
    plt.plot(df['elapsed_seconds'], df['memory_utilization'] * 100, label='memory utilization', color='blue')  # 乘100转为百分比
    
    plt.title('resource utilization')
    plt.xlabel('time(s)')
    plt.ylabel('utilization (%)')
    plt.legend()
    plt.grid(True)
    
    # 保存图表
    output_file = os.path.join(output_dir, f'{base_name}_resources_plot.png')
    plt.savefig(output_file, dpi=300)
    print(f"资源图表已保存: {output_file}")
    
    # 显示图表
    plt.show()
    
    # 绘制各类型操作数的堆叠图
    plt.figure(figsize=(12, 8))
    plt.stackplot(df['elapsed_seconds'], 
                 df['ycsb_ops'], 
                 df['kmeans_ops'], 
                 df['bank_ops'],
                 labels=['YCSB ops', 'KMEANS ops', 'BANK ops'],
                 colors=['#ff9999','#66b3ff','#99ff99'])
    
    plt.title('type of operations per time')
    plt.xlabel('time(s)')
    plt.ylabel('operations')
    plt.legend(loc='upper left')
    plt.grid(True)
    
    # 保存图表
    output_file = os.path.join(output_dir, f'{base_name}_operations_plot.png')
    plt.savefig(output_file, dpi=300)
    print(f"操作类型图表已保存: {output_file}")
    
    # 显示图表
    plt.show()

def plot_comparison_data(csv_files, output_dir='plots'):
    """对比绘制最新两个CSV文件的性能数据"""
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取两个CSV文件
    df1 = pd.read_csv(csv_files[0])  # 较老的文件 - rl_agent
    df2 = pd.read_csv(csv_files[1])  # 最新的文件 - static configuration
    
    # 创建对比图表1：请求总数和完成任务数
    plt.figure(figsize=(14, 8))
    
    # Request total (只显示一条，因为两个配置的request_total相同)
    plt.plot(df1['elapsed_seconds'], df1['request_total'], 
             label='request total', marker='o', markersize=3, 
             linestyle='-', color='#1f77b4', alpha=0.8)
    
    # RL Agent和Static Configuration的tasks completed对比
    plt.plot(df1['elapsed_seconds'], df1['total_ops'], 
             label='rl_agent - tasks completed', marker='x', markersize=3, 
             linestyle='-', color='#ff7f0e', alpha=0.8)
    plt.plot(df2['elapsed_seconds'], df2['total_ops'], 
             label='static configuration - tasks completed', marker='^', markersize=3, 
             linestyle='--', color='#2ca02c', alpha=0.8)
    
    plt.title('Performance Comparison: RL Agent vs Static Configuration')
    plt.xlabel('time(s)')
    plt.ylabel('operations')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 保存图表
    output_file = os.path.join(output_dir, 'comparison_performance_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"对比性能图表已保存: {output_file}")
    plt.show()
    
    # 创建对比图表2：CPU利用率单独对比
    plt.figure(figsize=(14, 8))
    
    # CPU利用率对比
    plt.plot(df1['elapsed_seconds'], df1['cpu_utilization'], 
             label='rl_agent - CPU utilization', color='#1f77b4', linestyle='-', 
             marker='o', markersize=2, alpha=0.8)
    plt.plot(df2['elapsed_seconds'], df2['cpu_utilization'], 
             label='static configuration - CPU utilization', color='#d62728', linestyle='--', 
             marker='s', markersize=2, alpha=0.8)
    
    plt.title('CPU Utilization Comparison: RL Agent vs Static Configuration')
    plt.xlabel('time(s)')
    plt.ylabel('CPU utilization (%)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 保存图表
    output_file = os.path.join(output_dir, 'comparison_cpu_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"对比CPU利用率图表已保存: {output_file}")
    plt.show()
    
    # 创建对比图表3：内存利用率单独对比
    plt.figure(figsize=(14, 8))
    
    # 内存利用率对比
    plt.plot(df1['elapsed_seconds'], df1['memory_utilization'] * 100, 
             label='rl_agent - memory utilization', color='#ff7f0e', linestyle='-', 
             marker='x', markersize=3, alpha=0.8)
    plt.plot(df2['elapsed_seconds'], df2['memory_utilization'] * 100, 
             label='static configuration - memory utilization', color='#2ca02c', linestyle='--', 
             marker='^', markersize=3, alpha=0.8)
    
    plt.title('Memory Utilization Comparison: RL Agent vs Static Configuration')
    plt.xlabel('time(s)')
    plt.ylabel('memory utilization (%)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 保存图表
    output_file = os.path.join(output_dir, 'comparison_memory_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"对比内存利用率图表已保存: {output_file}")
    plt.show()
    
    # 创建对比图表4：各类型操作数对比
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
    
    # RL Agent操作类型分布
    ax1.stackplot(df1['elapsed_seconds'], 
                  df1['ycsb_ops'], 
                  df1['kmeans_ops'], 
                  df1['bank_ops'],
                  labels=['YCSB ops', 'KMEANS ops', 'BANK ops'],
                  colors=['#ff9999','#66b3ff','#99ff99'],
                  alpha=0.8)
    ax1.set_title('RL Agent - Type of Operations per Time')
    ax1.set_xlabel('time(s)')
    ax1.set_ylabel('operations')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # Static Configuration操作类型分布
    ax2.stackplot(df2['elapsed_seconds'], 
                  df2['ycsb_ops'], 
                  df2['kmeans_ops'], 
                  df2['bank_ops'],
                  labels=['YCSB ops', 'KMEANS ops', 'BANK ops'],
                  colors=['#ffcc99','#99ccff','#ccff99'],
                  alpha=0.8)
    ax2.set_title('Static Configuration - Type of Operations per Time')
    ax2.set_xlabel('time(s)')
    ax2.set_ylabel('operations')
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # 保存图表
    output_file = os.path.join(output_dir, 'comparison_operations_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"对比操作类型图表已保存: {output_file}")
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='绘制FlexChain性能数据图表')
    parser.add_argument('--csv', type=str, help='CSV性能数据文件路径')
    parser.add_argument('--latest', action='store_true', help='使用最新的CSV文件')
    parser.add_argument('--compare', action='store_true', help='对比最新的两个CSV文件')
    parser.add_argument('--dir', type=str, default='logs/performance_data', help='CSV文件目录')
    parser.add_argument('--output', type=str, default='plots', help='图表输出目录')
    
    args = parser.parse_args()
    
    if args.csv:
        csv_file = args.csv
        plot_performance_data(csv_file, args.output)
    elif args.compare:
        # 查找最新的两个CSV文件
        files = glob.glob(f"{args.dir}/*.csv")
        if len(files) < 2:
            print(f"在目录 {args.dir} 中需要至少2个CSV文件才能进行对比")
            return
        
        # 按创建时间排序，获取最新的两个文件
        files_sorted = sorted(files, key=os.path.getctime)
        latest_two_files = files_sorted[-2:]  # 取最后两个（最新的两个）
        
        print(f"对比文件:")
        print(f"  RL Agent: {latest_two_files[0]}")
        print(f"  Static Configuration: {latest_two_files[1]}")
        
        plot_comparison_data(latest_two_files, args.output)
    elif args.latest:
        # 查找最新的CSV文件
        files = glob.glob(f"{args.dir}/*.csv")
        if not files:
            print(f"在目录 {args.dir} 中未找到CSV文件")
            return
        csv_file = max(files, key=os.path.getctime)
        print(f"使用最新的CSV文件: {csv_file}")
        plot_performance_data(csv_file, args.output)
    else:
        print("请指定CSV文件路径或使用--latest选项，或使用--compare选项进行对比")
        return

if __name__ == "__main__":
    main()