# rl_env.py
import time
import numpy as np
import gymnasium as gym
from gymnasium import spaces
import grpc
import logging
import threading
import csv
import os
from datetime import datetime

# 导入proto生成的Python模块
# 根据实际情况调整导入路径
import rl_agent_pb2
import rl_agent_pb2_grpc

try:
    from simulation_env import SimulationEnvironment
except ImportError:
    SimulationEnvironment = None

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FlexChainRL')

class FlexChainRLEnv(gym.Env):
    """FlexChain强化学习环境类"""
    
    def __init__(self, server_port=50055, history_length=5, data_log_dir='logs/performance_data', baseline_mode = False,simulation_mode=False, simulation_data_dir='processed_data'):
        super().__init__()
        
        # 初始化参数
        self.server_port = server_port
        self.baseline_mode = baseline_mode
        self.history_length = history_length
        self.server = None
        self.server_thread = None
        self.latest_states = []
        self.last_state = None
        self.current_state = np.array([0, 0, 0, 0, 0, 0, 1, 1, 0], dtype=np.float32)
        self.current_ts = 0
        self.last_ts = 0
        self.steps_count = 0
        self.state_condition = threading.Condition()
        self.processing = False

        self.simulation_mode = simulation_mode
        self.simulation_data_dir = simulation_data_dir
        if simulation_mode:
            self.sim_env = SimulationEnvironment(simulation_data_dir)
            logger.info("使用模拟环境模式")
        else:
            logger.info("使用真实环境模式")
        
        # 定义动作空间
        # 核心数调整 {-1, 0, 1}
        # 线程数调整 {-1, 0, 1}
        # evict阈值调整 {-1000, -100, 0, 100, 1000}
        self.core_adjustments = [-1, 0, 1]
        self.thread_adjustments = [-1, 0, 1]  
        # 大幅简化evict_threshold调整，因为它有延迟效应
        self.evict_thr_adjustments = [-2000, 0, 2000]  # 只保留3个选择
        
        # 动作空间大小：3*3*3 = 27
        self.action_space_n = spaces.Discrete(
            len(self.core_adjustments) * 
            len(self.thread_adjustments) * 
            len(self.evict_thr_adjustments)
        )
    
        # 重新定义观察空间 - 归一化后的范围
        # 定义观察空间 (状态空间)
        # 根据SystemState中的字段定义
        # [ycsb_ops, kmeans_ops, bank_ops, cpu_utilization, memory_utilization, 
        #  total_ops, core_count, sim_threads_per_core, evict_threshold]
        self.observation_space = spaces.Box(
            low=np.array([0, 0, 0, 0, 0, 0, 0, 0, 0]),
            high=np.array([1, 1, 1, 1, 1, 1, 1, 1, 1]),
            dtype=np.float32
        )
        

        
        # 让奖励的数值大一点
        self.scale_factor = 100

        # 奖励函数的权重
        self.w1 = 0.6  # 吞吐量变化的权重
        self.w2 = 0.2  # 内存利用率变化的权重
        self.w3 = 0.2  # CPU利用率变化的权重

        # 添加数据记录相关属性
        self.data_log_dir = data_log_dir
        self.performance_log_file = None
        self.performance_csv_writer = None
        self.start_time = None
        
        # 确保日志目录存在
        os.makedirs(self.data_log_dir, exist_ok=True)
        
        # 初始化性能日志文件
        self._init_performance_log()
        
        if not self.simulation_mode:
            # 启动gRPC服务器
            self._start_grpc_server()
            logger.info(f"FlexChain RL环境初始化完成，gRPC服务运行在端口 {self.server_port}")
        else:
            logger.info("FlexChain 模拟环境初始化完成")


    def _init_performance_log(self):
        """初始化性能数据日志文件"""
        # 创建带有时间戳的日志文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'performance_log_{timestamp}.csv'
        log_path = os.path.join(self.data_log_dir, log_filename)
        
        # 创建并打开日志文件
        self.performance_log_file = open(log_path, 'w', newline='')
        self.performance_csv_writer = csv.writer(self.performance_log_file)
        
        # 写入CSV文件头
        self.performance_csv_writer.writerow([
            'timestamp', 'elapsed_seconds', 'ycsb_ops', 'kmeans_ops', 'bank_ops', 
            'request_total', 'total_ops', 'cpu_utilization', 'memory_utilization',
            'core_count', 'threads_per_core', 'evict_threshold'
        ])
        
        # 记录开始时间
        self.start_time = time.time()
        
        logger.info(f"性能数据日志已初始化: {log_path}")

    def _log_performance_data(self, state_proto):
        """记录性能数据到CSV文件"""
        if self.performance_csv_writer is None:
            logger.warning("性能数据日志未初始化，无法记录数据")
            return
        
        current_time = time.time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elapsed_seconds = current_time - self.start_time
        
        # 计算请求总数
        request_total = state_proto.ycsb_ops + state_proto.kmeans_ops + state_proto.bank_ops
        
        # 记录数据行
        self.performance_csv_writer.writerow([
            timestamp,
            round(elapsed_seconds, 2),
            state_proto.ycsb_ops,
            state_proto.kmeans_ops, 
            state_proto.bank_ops,
            request_total,
            state_proto.total_ops,
            state_proto.cpu_utilization,
            state_proto.memory_utilization,
            state_proto.core_count,
            state_proto.sim_threads_per_core,
            state_proto.evict_threshold
        ])
        
        # 确保数据被写入文件
        self.performance_log_file.flush()
        
        # 记录日志
        logger.debug(f"记录性能数据: 时间={timestamp}, 请求总数={request_total}, 完成任务数={state_proto.total_ops}")

    def _start_grpc_server(self):
        """启动gRPC服务器用于接收系统状态"""
        from concurrent import futures
        import threading
        
        # 创建一个实现proto中定义的服务的类
        class RLAgentServicer(rl_agent_pb2_grpc.RLAgentServicer):
            def __init__(self, env):
                self.env = env
            
            def SendSystemStates(self, request, context):
                """接收系统状态并存储"""
                states = request.states
                if states:
                    # 将proto消息转换为内部状态表示
                    for state in states:
                        self.env._add_state(state)
                    
                    logger.info(f"\n接收到 {len(states)} 个系统状态")
                self.env.processing = True
                # 获取并返回最新的配置
                config = self.env._get_latest_config()
                return config
            
            def GetSystemConfig(self, request, context):
                """返回最新的系统配置"""
                return self.env._get_latest_config()
        
        # 创建gRPC服务器
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        rl_agent_pb2_grpc.add_RLAgentServicer_to_server(
            RLAgentServicer(self), self.server
        )
        
        # 在指定端口上启动服务器
        self.server.add_insecure_port(f'[::]:{self.server_port}')
        self.server.start()
        
        # 将服务器放在单独的线程中，避免阻塞主线程
        def server_thread_func():
            try:
                self.server.wait_for_termination()
            except Exception as e:
                logger.error(f"gRPC服务器异常: {e}")
        
        self.server_thread = threading.Thread(target=server_thread_func)
        self.server_thread.daemon = True
        self.server_thread.start()
    
    def _normalize_state(self, state_array):
        """归一化状态到合理范围"""
        normalized_state = np.zeros_like(state_array)
        
        # 归一化请求数 (0-20000范围)
        normalized_state[0] = min(state_array[0] / 20000.0, 1.0)  # ycsb_ops
        normalized_state[1] = min(state_array[1] / 2000.0, 1.0)   # kmeans_ops  
        normalized_state[2] = min(state_array[2] / 2000.0, 1.0)   # bank_ops
        
        # CPU利用率已经是0-100，转换为0-1
        normalized_state[3] = state_array[3] / 100.0
        
        # 内存利用率已经是0-1
        normalized_state[4] = state_array[4]
        
        # 归一化total_ops (0-20000范围)
        normalized_state[5] = min(state_array[5] / 20000.0, 1.0)
        
        # 归一化core_count (2-8范围，大多数情况不会超过8核)
        normalized_state[6] = (state_array[6] - 2.0) / 6.0
        
        # 归一化线程数 (1-4范围，实际很少超过4)
        normalized_state[7] = (state_array[7] - 1.0) / 3.0
        
        # 归一化evict_threshold (390000-400000范围)
        normalized_state[8] = (state_array[8] - 390000.0) / 10000.0
        
        return normalized_state

    def _add_state(self, state_proto):
        """添加一个新的系统状态到历史中"""
        # 转换proto消息为numpy数组
        raw_state = np.array([
            state_proto.ycsb_ops,
            state_proto.kmeans_ops, 
            state_proto.bank_ops,
            state_proto.cpu_utilization,
            state_proto.memory_utilization,
            state_proto.total_ops,
            state_proto.core_count,
            state_proto.sim_threads_per_core,
            state_proto.evict_threshold
        ], dtype=np.float32)
        
        # 归一化状态
        state_array = self._normalize_state(raw_state)
        
        # 更新状态
        self.last_state = self.current_state
        self.current_state = state_array
        self.current_ts = state_proto.timestamp
        
        # 存储原始状态用于计算奖励
        self.raw_current_state = raw_state
        self.raw_last_state = getattr(self, 'raw_current_state', raw_state.copy())
        
        # 将状态添加到历史中
        self.latest_states.append(state_array)
        if len(self.latest_states) > self.history_length:
            self.latest_states = self.latest_states[-self.history_length:]

        # 记录性能数据到CSV文件
        self._log_performance_data(state_proto)


    def _get_latest_config(self):
        """获取最新的系统配置"""
        while self.processing:
            time.sleep(0.1)
        # return self.current_config
        if hasattr(self, 'current_config'):
            config = self.current_config
            # 返回配置后清除，确保只应用一次
            delattr(self, 'current_config')
            logger.info(f"返回配置: core_adj={config.core_adjustment}, thread_adj={config.thread_adjustment}, evict_thr_adj={config.evict_thr_adjustment}")
            return config
        else:
            # 默认不做任何调整
            config = rl_agent_pb2.SystemConfig()
            config.core_adjustment = 0
            config.thread_adjustment = 0
            config.evict_thr_adjustment = 0
            return config

    def _action_to_adjustments(self, action):
        """将动作ID转换为具体的调整参数"""
        # 计算动作对应的各个维度上的索引
        n_thread_adj = len(self.thread_adjustments)
        n_evict_adj = len(self.evict_thr_adjustments)
        
        core_idx = action // (n_thread_adj * n_evict_adj)
        thread_idx = (action % (n_thread_adj * n_evict_adj)) // n_evict_adj
        evict_idx = action % n_evict_adj
        
        # 返回具体的调整值
        return (
            self.core_adjustments[core_idx],
            self.thread_adjustments[thread_idx],
            self.evict_thr_adjustments[evict_idx]
        )

    def reset(self, seed=None, options=None):
        """重置环境状态"""
        super().reset(seed=seed)
        
        if self.simulation_mode:
            # 模拟环境重置
            initial_state = self.sim_env.reset()
            self.current_state = self._normalize_state(initial_state)
            self.raw_current_state = initial_state
            self.latest_states = [self.current_state]
            observation = self.current_state
        else:
            # 等待获取初始状态（原有逻辑）
            while not self.latest_states:
                logger.info("等待初始系统状态...")
                time.sleep(1)
            observation = self.latest_states[-1]
        
        self.steps_count = 0
        info = {}
        
        return observation, info

    def step(self, action):
        """执行一个动作并等待系统响应"""
        self.steps_count += 1
        
        # 保存执行动作前的状态
        pre_action_state = self.current_state.copy() if self.current_state is not None else None
        
        # 将动作ID转换为具体的调整参数
        core_adj, thread_adj, evict_thr_adj = self._action_to_adjustments(action)

        # 使用原始状态进行动作有效性检查
        if hasattr(self, 'raw_current_state') and self.raw_current_state is not None:
            current_core_count = self.raw_current_state[6]
            current_sim_count = self.raw_current_state[7]
            current_evict_thr = self.raw_current_state[8]
            
            # 更严格的约束检查
            if current_core_count + core_adj < 2:
                logger.warning(f"动作修正: 核心数调整 {core_adj} -> 0 (当前={current_core_count})")
                core_adj = 0
                
            if current_sim_count + thread_adj < 1:
                logger.warning(f"动作修正: 线程数调整 {thread_adj} -> 0 (当前={current_sim_count})")
                thread_adj = 0
                
            if (current_sim_count + thread_adj) * (current_core_count + core_adj) > 31:
                logger.warning(f"动作修正: 总线程数超限，重置调整为0")
                thread_adj = 0
                core_adj = 0
                
            if current_evict_thr + evict_thr_adj < 390000 or current_evict_thr + evict_thr_adj > 398000:
                logger.warning(f"动作修正: evict_thr调整 {evict_thr_adj} -> 0 (当前={current_evict_thr})")
                evict_thr_adj = 0

        logger.info(f"执行动作: core_adj={core_adj}, thread_adj={thread_adj}, evict_thr_adj={evict_thr_adj}")
        
        # 应用配置变更
        self._apply_config(core_adj, thread_adj, evict_thr_adj)
        
        if self.simulation_mode:
            # 模拟环境步进
            new_state, sim_reward = self.sim_env.step(core_adj, thread_adj, evict_thr_adj)
            
            # 更新状态
            self.last_state = self.current_state.copy() if self.current_state is not None else None
            self.raw_last_state = getattr(self, 'raw_current_state', new_state.copy())
            self.raw_current_state = new_state
            self.current_state = self._normalize_state(new_state)
            self.latest_states.append(self.current_state)
            if len(self.latest_states) > self.history_length:
                self.latest_states = self.latest_states[-self.history_length:]
            
            reward = self._calculate_reward() if pre_action_state is not None else 0
            
        else:
            # 真实环境等待逻辑（保持原有代码）
            max_wait_time = 10
            start_time = time.time()
            state_updated = False
            
            while not state_updated and (time.time() - start_time < max_wait_time):
                if hasattr(self, 'current_ts') and hasattr(self, 'last_ts') and self.current_ts != self.last_ts:
                    state_updated = True
                    self.last_ts = self.current_ts
                else:
                    time.sleep(0.5)
            
            if not state_updated:
                logger.warning(f"等待状态更新超时 ({max_wait_time}秒)")
                reward = -1.0
            else:
                reward = self._calculate_reward() if pre_action_state is not None else 0
        
        # 检查是否需要重置到baseline
        if self.steps_count % 500 == 0:  # 每500步重置一次
            logger.info("重置到baseline配置")
            self._apply_config(-int(self.raw_current_state[6]-4), 
                            -int(self.raw_current_state[7]-1), 
                            int(390000-self.raw_current_state[8]))
        
        terminated = False
        truncated = False
        
        observation = self.latest_states[-1] if self.latest_states else np.zeros(self.observation_space.shape)
        info = {
            'steps': self.steps_count,
            'last_adjustments': {'core': core_adj, 'thread': thread_adj, 'evict_thr': evict_thr_adj},
            'wait_time': time.time() - start_time,
            'raw_state': self.raw_current_state.tolist() if hasattr(self, 'raw_current_state') else None
        }
        
        return observation, reward, terminated, truncated, info
    
    def action_space(self):
        """返回动作空间的大小"""
        return self.action_space_n.n

    def _apply_config(self, core_adj, thread_adj, evict_thr_adj):
        """应用配置变更，将在下一个状态请求时返回"""
        # 存储当前的配置，将在下一次SendSystemStates或GetSystemConfig请求时返回
        if self.simulation_mode:
            # 模拟环境不需要gRPC配置
            return
        if self.baseline_mode:
            self.current_config = rl_agent_pb2.SystemConfig(
                core_adjustment=0,
                thread_adjustment=0,
                evict_thr_adjustment=0
            )
        else:
            self.current_config = rl_agent_pb2.SystemConfig(
                core_adjustment=core_adj,
                thread_adjustment=thread_adj,
                evict_thr_adjustment=evict_thr_adj
            )
        self.processing = False

    def _calculate_reward(self):
        """计算奖励值，基于绝对性能而非变化量"""
        if not hasattr(self, 'raw_last_state') or not hasattr(self, 'raw_current_state'):
            return 0.0
        
        # 使用原始状态计算奖励
        s_t1 = self.raw_current_state
        
        # 1. 吞吐量完成率奖励 (核心指标)
        total_requests = s_t1[0] + s_t1[1] + s_t1[2]  # 总请求数
        completed_ops = s_t1[5]  # total_ops
        
        if total_requests > 0:
            completion_rate = min(completed_ops / total_requests, 1.0)
            # 使用非线性奖励，完成率越高奖励越大
            throughput_reward = completion_rate ** 2 * 10.0  # 0-10分
        else:
            throughput_reward = 1.0  # 没有请求时给基础分
        
        # 2. 吞吐量改善奖励 (相对于上一步)
        if hasattr(self, 'raw_last_state'):
            throughput_change = s_t1[5] - self.raw_last_state[5]
            if throughput_change > 0:
                improvement_reward = min(throughput_change / max(total_requests, 1), 1.0) * 5.0
            else:
                improvement_reward = max(throughput_change / max(total_requests, 1), -0.5) * 2.0
        else:
            improvement_reward = 0.0
        
        # 3. CPU效率奖励 (目标70-85%)
        cpu_util = s_t1[3] / 100.0
        if 0.70 <= cpu_util <= 0.85:
            cpu_reward = 3.0
        elif 0.60 <= cpu_util <= 0.90:
            # 距离理想区间的距离
            if cpu_util < 0.70:
                distance = 0.70 - cpu_util
            else:
                distance = cpu_util - 0.85
            cpu_reward = max(0, 3.0 - distance * 10)  # 线性衰减
        else:
            cpu_reward = 0.0
        
        # 4. 内存效率奖励 (目标80-95%)
        memory_util = s_t1[4]
        if 0.80 <= memory_util <= 0.95:
            memory_reward = 2.0
        elif 0.70 <= memory_util <= 0.99:
            if memory_util < 0.80:
                distance = 0.80 - memory_util
            else:
                distance = memory_util - 0.95
            memory_reward = max(0, 2.0 - distance * 5)
        else:
            memory_reward = 0.0
        
        # 5. 资源效率奖励 (避免过度分配)
        core_count = s_t1[6]
        thread_count = s_t1[7]
        total_threads = core_count * thread_count
        
        if total_requests > 0:
            # 理想情况：每个线程处理50-200个请求
            requests_per_thread = total_requests / total_threads
            if 50 <= requests_per_thread <= 200:
                efficiency_reward = 2.0
            elif 20 <= requests_per_thread <= 300:
                efficiency_reward = 1.0
            else:
                efficiency_reward = 0.0
        else:
            # 没有请求时，线程数越少越好
            efficiency_reward = max(0, 2.0 - (total_threads - 4) * 0.2)
        
        # 6. 稳定性奖励 (避免频繁大幅调整)
        stability_reward = 1.0  # 基础稳定分
        
        # 综合奖励 (总分0-23分)
        total_reward = (
            throughput_reward * 0.35 +     # 35% - 吞吐量完成率
            improvement_reward * 0.25 +    # 25% - 吞吐量改善  
            cpu_reward * 0.2 +             # 20% - CPU效率
            memory_reward * 0.1 +          # 10% - 内存效率
            efficiency_reward * 0.05 +     # 5% - 资源效率
            stability_reward * 0.05        # 5% - 稳定性
        )
        
        # 记录详细信息用于调试
        if total_requests > 0:
            logger.info(f"奖励详情: 请求={total_requests}, 完成={completed_ops}, "
                    f"完成率={completion_rate:.3f}({throughput_reward:.2f}), "
                    f"改善={throughput_change if hasattr(self, 'raw_last_state') else 0}({improvement_reward:.2f}), "
                    f"CPU={cpu_util:.3f}({cpu_reward:.2f}), "
                    f"内存={memory_util:.3f}({memory_reward:.2f}), "
                    f"效率={efficiency_reward:.2f}, 总奖励={total_reward:.2f}")
        
        return total_reward
    
    def close(self):
        """关闭环境，停止gRPC服务器，并关闭日志文件"""
        # 关闭性能日志文件
        if self.performance_log_file:
            self.performance_log_file.close()
            logger.info("性能数据日志文件已关闭")

        """关闭环境，停止gRPC服务器"""
        if self.server:
            self.server.stop(0)
            logger.info("gRPC服务器已停止")
            
        super().close()