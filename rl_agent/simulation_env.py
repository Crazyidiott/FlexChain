import pandas as pd
import numpy as np
import os
import random
from pathlib import Path
import logging

logger = logging.getLogger('FlexChainRL.Simulation')

class SimulationEnvironment:
    """
    基于静态数据的FlexChain模拟环境 - 修正版
    
    核心思想：
    1. 直接使用CSV中的真实数据进行状态预测
    2. 通过匹配、插值、外推来处理不同配置
    3. 约束违规时取消调整而不是强行修正
    """
    
    def __init__(self, data_dir='processed_data', workload_sequence=None):
        self.data_dir = Path(data_dir)
        self.workload_sequence = workload_sequence or ['kmeans', 'ycsb', 'small']
        
        # 加载所有数据
        self.data = self._load_all_data()
        
        # 当前状态
        self.current_workload = random.choice(self.workload_sequence)
        self.current_workload_size = random.choice([1, 2, 3])
        self.workload_duration = 0  # 当前workload持续时间
        self.max_workload_duration = 20  # 每种workload最多持续20步
        
        # 系统配置状态
        self.current_core_count = 4  # 初始核心数
        self.current_thread_count = 1  # 初始线程数
        self.current_evict_thr = 396000  # 初始evict阈值
        
        # 性能历史（用于模拟系统惯性）
        self.performance_history = []
        self.max_history_length = 5
        
        # 噪声参数（模拟真实系统的波动）
        self.noise_level = 0.05  # 5%的随机噪声
        
        logger.info(f"模拟环境初始化完成，当前workload: {self.current_workload}_size_{self.current_workload_size}")
    
    def _load_all_data(self):
        """加载所有CSV数据文件"""
        data = {}
        
        workload_types = ['ycsb', 'kmeans', 'small']
        sizes = [1, 2, 3]
        
        for workload in workload_types:
            data[workload] = {}
            for size in sizes:
                filename = f"{workload}_size_{size}.csv"
                file_path = self.data_dir / filename
                
                if file_path.exists():
                    df = pd.read_csv(file_path)
                    # 按核心数分组，方便查找
                    data[workload][size] = self._group_by_cores(df)
                    logger.info(f"加载数据: {filename}, {len(df)} 行")
                else:
                    logger.warning(f"数据文件不存在: {filename}")
                    
        return data
    
    def _group_by_cores(self, df):
        """按核心数分组数据，便于快速查找"""
        grouped = {}
        for core_count in df['core_count'].unique():
            core_data = df[df['core_count'] == core_count]
            grouped[core_count] = core_data
        return grouped
    
    def reset(self):
        """重置环境到初始状态"""
        # 随机选择workload
        self.current_workload = random.choice(self.workload_sequence)
        self.current_workload_size = random.choice([1, 2, 3])
        self.workload_duration = 0
        
        # 重置系统配置到默认值
        self.current_core_count = 4
        self.current_thread_count = 1
        self.current_evict_thr = 396000
        
        # 清空历史
        self.performance_history = []
        
        # 获取初始状态
        initial_state = self._get_current_state()
        
        logger.info(f"环境重置，workload: {self.current_workload}_size_{self.current_workload_size}")
        return initial_state
    
    def step(self, core_adj, thread_adj, evict_thr_adj):
        """
        执行一个动作，返回新状态和奖励
        
        Args:
            core_adj: 核心数调整 (-1, 0, 1)
            thread_adj: 线程数调整 (-1, 0, 1)  
            evict_thr_adj: evict阈值调整 (-2000, 0, 2000)
        
        Returns:
            new_state: 新的状态向量
            reward: 即时奖励（暂时返回0，由环境计算）
        """
        # 1. 应用配置变更（修正版：约束违规时取消调整）
        self._apply_adjustments(core_adj, thread_adj, evict_thr_adj)
        
        # 2. 检查是否需要切换workload
        self._update_workload()
        
        # 3. 根据当前配置预测系统性能（修正版：直接使用真实数据）
        new_state = self._predict_state()
        
        # 4. 添加历史记录
        self._update_history(new_state)
        
        # 5. 增加持续时间
        self.workload_duration += 1
        
        logger.debug(f"模拟步进: cores={self.current_core_count}, threads={self.current_thread_count}, "
                    f"evict_thr={self.current_evict_thr}, workload={self.current_workload}")
        
        return new_state, 0.0  # 奖励由主环境计算
    
    def _apply_adjustments(self, core_adj, thread_adj, evict_thr_adj):
        """应用资源调整，约束违规时取消调整 - 修正版"""
        
        # 计算调整后的值
        new_core_count = self.current_core_count + core_adj
        new_thread_count = self.current_thread_count + thread_adj
        new_evict_thr = self.current_evict_thr + evict_thr_adj
        
        # 约束检查 - 如果违反约束则取消相应调整
        if new_core_count < 2:
            logger.debug(f"约束违规：核心数不能小于2，取消核心调整 {core_adj}")
            core_adj = 0
            
        if new_thread_count < 1:
            logger.debug(f"约束违规：线程数不能小于1，取消线程调整 {thread_adj}")
            thread_adj = 0
            
        # 重新计算（因为可能有调整被取消了）
        new_core_count = self.current_core_count + core_adj
        new_thread_count = self.current_thread_count + thread_adj
        
        if new_core_count * new_thread_count > 31:
            logger.debug(f"约束违规：总线程数不能超过31，取消核心和线程调整")
            thread_adj = 0
            core_adj = 0
            
        if new_evict_thr < 390000 or new_evict_thr > 398000:
            logger.debug(f"约束违规：evict_thr超出范围，取消evict调整 {evict_thr_adj}")
            evict_thr_adj = 0
        
        # 应用有效的调整
        self.current_core_count += core_adj
        self.current_thread_count += thread_adj
        self.current_evict_thr += evict_thr_adj
        
        if core_adj != 0 or thread_adj != 0 or evict_thr_adj != 0:
            logger.debug(f"应用调整: core+={core_adj}, thread+={thread_adj}, evict_thr+={evict_thr_adj}")
    
    def _update_workload(self):
        """更新workload类型（模拟真实环境中的workload变化）"""
        # 每个workload持续一定时间后切换
        if self.workload_duration >= self.max_workload_duration:
            old_workload = f"{self.current_workload}_size_{self.current_workload_size}"
            
            # 30%概率改变workload类型，70%概率改变size
            if random.random() < 0.3:
                self.current_workload = random.choice(self.workload_sequence)
            else:
                self.current_workload_size = random.choice([1, 2, 3])
            
            self.workload_duration = 0
            
            new_workload = f"{self.current_workload}_size_{self.current_workload_size}"
            if old_workload != new_workload:
                logger.info(f"Workload切换: {old_workload} -> {new_workload}")
    
    def _predict_state(self):
        """
        基于历史数据预测状态 - 修正版
        
        逻辑：
        1. 找到与当前配置最接近的历史数据点
        2. 如果找不到完全匹配，从相近配置中插值
        3. 添加适当噪声模拟系统波动
        """
        workload_data = self.data.get(self.current_workload, {}).get(self.current_workload_size, {})
        
        if not workload_data:
            logger.warning(f"没有找到workload数据: {self.current_workload}_size_{self.current_workload_size}")
            return self._get_default_state()
        
        # 方法1：寻找完全匹配的配置
        if self.current_core_count in workload_data:
            core_data = workload_data[self.current_core_count]
            # 从该核心数的所有样本中随机选择一个（模拟自然波动）
            if len(core_data) > 0:
                sample = core_data.sample(1).iloc[0]
                return self._sample_to_state(sample)
        
        # 方法2：如果没有完全匹配，找最接近的两个核心数进行插值
        available_cores = sorted(workload_data.keys())
        
        if self.current_core_count < min(available_cores):
            # 当前核心数小于所有样本，使用最小的并外推
            sample = workload_data[min(available_cores)].sample(1).iloc[0]
            return self._sample_to_state(sample, extrapolate=True)
        
        if self.current_core_count > max(available_cores):
            # 当前核心数大于所有样本，使用最大的并外推
            sample = workload_data[max(available_cores)].sample(1).iloc[0]
            return self._sample_to_state(sample, extrapolate=True)
        
        # 在范围内插值
        lower_core = max([c for c in available_cores if c <= self.current_core_count])
        upper_core = min([c for c in available_cores if c >= self.current_core_count])
        
        if lower_core == upper_core:
            sample = workload_data[lower_core].sample(1).iloc[0]
            return self._sample_to_state(sample)
        
        # 线性插值
        lower_sample = workload_data[lower_core].sample(1).iloc[0]
        upper_sample = workload_data[upper_core].sample(1).iloc[0]
        
        # 插值权重
        weight = (self.current_core_count - lower_core) / (upper_core - lower_core)
        
        # 插值计算
        interpolated_state = self._interpolate_samples(lower_sample, upper_sample, weight)
        return interpolated_state
    
    def _sample_to_state(self, sample, extrapolate=False):
        """将CSV样本转换为状态向量"""
        state = np.array([
            sample['ycsb_ops'],
            sample['kmeans_ops'], 
            sample['bank_ops'],
            sample['cpu_utilization'],
            sample['memory_utilization'],
            sample['total_ops_interval'],  # 这就是真实的吞吐量数据
            self.current_core_count,       # 使用当前配置
            self.current_thread_count,     # 使用当前配置
            self.current_evict_thr         # 使用当前配置
        ], dtype=np.float32)
        
        # 如果是外推，调整性能指标
        if extrapolate:
            # 简单的线性外推
            core_ratio = self.current_core_count / sample['core_count']
            # 限制外推范围，避免过于极端的值
            ratio = min(1.5, max(0.5, core_ratio))
            state[5] *= ratio  # 吞吐量随核心数变化
            state[3] /= ratio  # CPU利用率反向变化
            # 确保在合理范围内
            state[3] = max(5.0, min(95.0, state[3]))
        
        return self._add_noise(state)
    
    def _interpolate_samples(self, sample1, sample2, weight):
        """在两个样本之间进行线性插值"""
        interpolated = np.array([
            sample1['ycsb_ops'] * (1-weight) + sample2['ycsb_ops'] * weight,
            sample1['kmeans_ops'] * (1-weight) + sample2['kmeans_ops'] * weight,
            sample1['bank_ops'] * (1-weight) + sample2['bank_ops'] * weight,
            sample1['cpu_utilization'] * (1-weight) + sample2['cpu_utilization'] * weight,
            sample1['memory_utilization'] * (1-weight) + sample2['memory_utilization'] * weight,
            sample1['total_ops_interval'] * (1-weight) + sample2['total_ops_interval'] * weight,
            self.current_core_count,    # 配置参数使用当前值
            self.current_thread_count,
            self.current_evict_thr
        ], dtype=np.float32)
        
        return self._add_noise(interpolated)
    
    def _add_noise(self, state):
        """添加随机噪声模拟真实系统的波动"""
        noisy_state = state.copy()
        
        # 对性能指标添加噪声（请求数、CPU、内存、吞吐量）
        performance_indices = [0, 1, 2, 3, 4, 5]  # ycsb_ops, kmeans_ops, bank_ops, cpu, memory, total_ops
        
        for idx in performance_indices:
            if idx in [3, 4]:  # CPU和内存利用率
                # 较小的噪声
                noise = np.random.normal(0, self.noise_level * 0.5)
            else:  # 请求数和吞吐量
                # 稍大的噪声
                noise = np.random.normal(0, self.noise_level)
            
            noisy_state[idx] = noisy_state[idx] * (1 + noise)
        
        # 确保值在合理范围内
        noisy_state[3] = max(5.0, min(95.0, noisy_state[3]))    # CPU 5-95%
        noisy_state[4] = max(0.7, min(0.99, noisy_state[4]))    # Memory 70-99%
        noisy_state[0] = max(0, noisy_state[0])                 # 请求数不能为负
        noisy_state[1] = max(0, noisy_state[1])
        noisy_state[2] = max(0, noisy_state[2])
        noisy_state[5] = max(0, noisy_state[5])                 # 吞吐量不能为负
        
        return noisy_state
    
    def _apply_system_inertia(self, predicted_state):
        """应用系统惯性：性能不会立即跳跃，而是逐渐变化"""
        if not self.performance_history:
            return predicted_state
        
        # 获取最近的性能
        last_performance = self.performance_history[-1]
        
        # 惯性系数：70%保持惯性，30%向预测值变化
        inertia_factor = 0.7
        change_factor = 0.3
        
        # 只对性能指标应用惯性（不包括配置参数）
        performance_indices = [0, 1, 2, 3, 4, 5]  # 请求数、CPU、内存、吞吐量
        
        inertial_state = predicted_state.copy()
        for idx in performance_indices:
            inertial_state[idx] = (last_performance[idx] * inertia_factor + 
                                 predicted_state[idx] * change_factor)
        
        return inertial_state
    
    def _update_history(self, new_state):
        """更新性能历史记录"""
        self.performance_history.append(new_state.copy())
        if len(self.performance_history) > self.max_history_length:
            self.performance_history.pop(0)
    
    def _get_current_state(self):
        """获取当前状态（用于reset）"""
        return self._predict_state()
    
    def _get_default_state(self):
        """获取默认状态（当没有数据时使用）"""
        # 根据workload类型返回合理的默认值
        if self.current_workload == 'ycsb':
            ycsb_ops, kmeans_ops, bank_ops = 100, 0, 0
        elif self.current_workload == 'kmeans':
            ycsb_ops, kmeans_ops, bank_ops = 0, 100, 0
        else:  # small bank
            ycsb_ops, kmeans_ops, bank_ops = 0, 0, 100
        
        return np.array([
            ycsb_ops, kmeans_ops, bank_ops,
            20.0,  # CPU利用率
            0.85,  # 内存利用率
            80,    # 吞吐量
            self.current_core_count,
            self.current_thread_count,
            self.current_evict_thr
        ], dtype=np.float32)
    
    def get_info(self):
        """获取环境信息"""
        return {
            'current_workload': self.current_workload,
            'workload_size': self.current_workload_size,
            'workload_duration': self.workload_duration,
            'core_count': self.current_core_count,
            'thread_count': self.current_thread_count,
            'evict_threshold': self.current_evict_thr,
            'performance_history_length': len(self.performance_history)
        }