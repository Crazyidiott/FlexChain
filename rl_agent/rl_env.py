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

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FlexChainRL')

class FlexChainRLEnv(gym.Env):
    """FlexChain强化学习环境类"""
    
    def __init__(self, server_port=50055, history_length=5, data_log_dir='logs/performance_data'):
        super().__init__()
        
        # 初始化参数
        self.server_port = server_port
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
        
        # 定义动作空间
        # 核心数调整 {-1, 0, 1}
        # 线程数调整 {-1, 0, 1}
        # evict阈值调整 {-1000, -100, 0, 100, 1000}
        self.core_adjustments = [-1, 0, 1]
        self.thread_adjustments = [-1, 0, 1]
        self.evict_thr_adjustments = [-1000, -100, 0, 100, 1000]
        
        # 动作空间为三个子空间的笛卡尔积
        self.action_space_n = spaces.Discrete(
            len(self.core_adjustments) * 
            len(self.thread_adjustments) * 
            len(self.evict_thr_adjustments)
        )
        
        # 定义观察空间 (状态空间)
        # 根据SystemState中的字段定义
        # [ycsb_ops, kmeans_ops, bank_ops, cpu_utilization, memory_utilization, 
        #  total_ops, core_count, sim_threads_per_core, evict_threshold]
        self.observation_space = spaces.Box(
            low=np.array([0, 0, 0, 0, 0, 0, 1, 1, 400000]),
            high=np.array([np.inf, np.inf, np.inf, 100, 1, np.inf, np.inf, np.inf, np.inf]),
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
        
        # 启动gRPC服务器
        self._start_grpc_server()
        logger.info(f"FlexChain RL环境初始化完成，gRPC服务运行在端口 {self.server_port}")

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
                self.processing = True
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

    def _add_state(self, state_proto):
        """添加一个新的系统状态到历史中"""
        # 转换proto消息为numpy数组
        state_array = np.array([
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
        
        # 更新状态
        self.last_state = self.current_state
        self.current_state = state_array
        self.current_ts = state_proto.timestamp
        
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
        
        # 等待获取初始状态
        while not self.latest_states:
            logger.info("等待初始系统状态...")
            time.sleep(1)
        
        self.steps_count = 0
        
        # 返回最新状态作为初始观察
        observation = self.latest_states[-1]
        info = {}
        
        return observation, info

    def step(self, action):
        """执行一个动作并等待系统响应"""
        self.steps_count += 1
        
        # 保存执行动作前的状态
        pre_action_state = self.current_state.copy() if self.current_state is not None else None
        
        # 将动作ID转换为具体的调整参数
        core_adj, thread_adj, evict_thr_adj = self._action_to_adjustments(action)

        # 不可用动作筛选
        if self.current_state is not None:
            current_core_count = self.current_state[6]  # core_count的索引
            current_sim_count = self.current_state[7]  # sim_threads_per_core的索引
            current_evict_thr = self.current_state[8]  # evict_threshold的索引
            if current_core_count + core_adj < 2 or current_core_count + core_adj > 32:  # 确保核心数不少于2
                logger.warning(f"不可行动作: 当前核心数={current_core_count}, 尝试调整={core_adj}")
                # 方式1: 返回大的负奖励，但不实际应用动作
                # return self.current_state.copy(), -10000.0, False, False, {"invalid_action": True}
                # 方式2: 修改动作为安全的动作
                core_adj = 0  # 或者 core_adj = max(1 - current_core_count, core_adj)
            if current_sim_count + thread_adj < 1 or current_sim_count * current_core_count > 31:  # 确保线程数不少于1
                logger.warning(f"不可行动作: 当前线程数={current_sim_count},尝试调整thread={core_adj}, 尝试调整core={thread_adj}")
                # 方式1: 返回大的负奖励，但不实际应用动作
                # return self.current_state.copy(), -10000.0, False, False, {"invalid_action": True}
                thread_adj = 0
            if current_evict_thr + evict_thr_adj < 100 or current_evict_thr + evict_thr_adj > 400000 :  # eveict_threshold的范围
                logger.warning(f"不可行动作: 当前evict_thr={current_evict_thr}, 尝试调整={evict_thr_adj}")
                # 方式1: 返回大的负奖励，但不实际应用动作
                # return self.current_state.copy(), -10000.0, False, False, {"invalid_action": True}
                evict_thr_adj = 0
                

        logger.info(f"执行动作: core_adj={core_adj}, thread_adj={thread_adj}, evict_thr_adj={evict_thr_adj}\n当前状态={self.current_state}")
        
        # 应用配置变更
        self._apply_config(core_adj, thread_adj, evict_thr_adj)
        
        # 动态等待系统状态更新
        # 使用超时机制确保不会无限等待
        max_wait_time = 30  # 最长等待30秒
        start_time = time.time()
        state_updated = False
        
        while not state_updated and (time.time() - start_time < max_wait_time):
            # 检查是否有新状态到达
            if self.current_state is not None and not self.last_ts == self.current_ts:
                state_updated = True
                self.last_ts = self.current_ts
            else:
                time.sleep(1)  # 短暂等待后再次检查
        
        if not state_updated:
            logger.warning(f"等待状态更新超时 ({max_wait_time}秒)，使用最新可用状态")
        
        # 计算奖励
        reward = self._calculate_reward() if pre_action_state is not None and self.current_state is not None else 0
        
        # 在持续环境中，我们定义"完成"的概念
        # 例如，如果达到预定的步数限制，或者出现特定条件
        max_steps = 1000  # 示例：最大步数
        terminated = False
        truncated = False # self.steps_count >= max_steps
        
        # 返回最新观察、奖励等
        observation = self.latest_states[-1] if self.latest_states else np.zeros(self.observation_space.shape)
        info = {
            'steps': self.steps_count,
            'last_adjustments': {
                'core': core_adj,
                'thread': thread_adj,
                'evict_thr': evict_thr_adj
            },
            'wait_time': time.time() - start_time
        }
    
        return observation, reward, terminated, truncated, info

    def action_space(self):
        """返回动作空间的大小"""
        return self.action_space_n.n

    def _apply_config(self, core_adj, thread_adj, evict_thr_adj):
        """应用配置变更，将在下一个状态请求时返回"""
        # 存储当前的配置，将在下一次SendSystemStates或GetSystemConfig请求时返回
        self.current_config = rl_agent_pb2.SystemConfig(
            core_adjustment=core_adj,
            thread_adjustment=thread_adj,
            evict_thr_adjustment=evict_thr_adj
        )
        self.processing = False

    def _calculate_reward(self):
        """计算奖励值，基于状态变化"""
        # 获取前一个状态和当前状态
        s_t = self.last_state
        s_t1 = self.current_state
        
        # 提取相关指标
        T_t = s_t[5]  # total_ops
        T_t1 = s_t1[5]
        
        # 使用请求数作为T_max进行归一化

        T_max_t = max(s_t[0]+s_t[1]+s_t[2], 1)  # 避免除以0
        T_max_t1 = max(s_t1[0]+s_t1[1]+s_t1[2], 1)
        
        MU_t = s_t[4]  # memory_utilization
        MU_t1 = s_t1[4]
        
        CU_t = s_t[3] / 100.0  # cpu_utilization / 100.0 转为0-1的范围
        CU_t1 = s_t1[3] / 100.0
        
        # 计算奖励
        # R_t = w_1(T_{t+1}/T_{max} - T_t/T_{max}) + w_2(MU_{t+1}-MU_t) + w_3(CU_{t+1}-CU_t)
        throughput_change = (T_t1/T_max_t1) - (T_t/T_max_t)
        memory_util_change = MU_t1 - MU_t
        cpu_util_change = CU_t1 - CU_t
        
        reward = self.scale_factor * (self.w1 * throughput_change + self.w2 * memory_util_change + self.w3 * cpu_util_change)
        
        logger.info(f"奖励计算: throughput_change={throughput_change}, memory_util_change={memory_util_change}, "
                   f"cpu_util_change={cpu_util_change}, reward={reward}")
        
        return reward

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