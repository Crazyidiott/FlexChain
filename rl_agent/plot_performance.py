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
    plt.plot(df['elapsed_seconds'], df['request_total'], label='请求总数', marker='o', markersize=3, linestyle='-')
    plt.plot(df['elapsed_seconds'], df['total_ops'], label='完成任务数', marker='x', markersize=3, linestyle='-')
    
    # 添加标题和标签
    plt.title('系统性能随时间变化')
    plt.xlabel('时间 (秒)')
    plt.ylabel('操作数')
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
    plt.plot(df['elapsed_seconds'], df['cpu_utilization'], label='CPU利用率', color='red')
    plt.plot(df['elapsed_seconds'], df['memory_utilization'] * 100, label='内存利用率', color='blue')  # 乘100转为百分比
    
    plt.title('资源利用率随时间变化')
    plt.xlabel('时间 (秒)')
    plt.ylabel('利用率 (%)')
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
                 labels=['YCSB操作', 'KMEANS操作', 'BANK操作'],
                 colors=['#ff9999','#66b3ff','#99ff99'])
    
    plt.title('不同类型操作数随时间变化')
    plt.xlabel('时间 (秒)')
    plt.ylabel('操作数')
    plt.legend(loc='upper left')
    plt.grid(True)
    
    # 保存图表
    output_file = os.path.join(output_dir, f'{base_name}_operations_plot.png')
    plt.savefig(output_file, dpi=300)
    print(f"操作类型图表已保存: {output_file}")
    
    # 显示图表
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='绘制FlexChain性能数据图表')
    parser.add_argument('--csv', type=str, help='CSV性能数据文件路径')
    parser.add_argument('--latest', action='store_true', help='使用最新的CSV文件')
    parser.add_argument('--dir', type=str, default='logs/performance_data', help='CSV文件目录')
    parser.add_argument('--output', type=str, default='plots', help='图表输出目录')
    
    args = parser.parse_args()
    
    if args.csv:
        csv_file = args.csv
    elif args.latest:
        # 查找最新的CSV文件
        files = glob.glob(f"{args.dir}/*.csv")
        if not files:
            print(f"在目录 {args.dir} 中未找到CSV文件")
            return
        csv_file = max(files, key=os.path.getctime)
        print(f"使用最新的CSV文件: {csv_file}")
    else:
        print("请指定CSV文件路径或使用--latest选项")
        return
    
    plot_performance_data(csv_file, args.output)

if __name__ == "__main__":
    main()