# -*- coding: utf-8 -*-
from __future__ import division
import argparse
import os
import torch
from tqdm import tqdm
import numpy as np
import time
from datetime import datetime, timedelta
import copy

from agent import Agent
from rl_env import FlexChainRLEnv
from memory import createReplayMemory
from test_rainbow import test_during_training

import bz2
import pickle
import logging


os.makedirs("logs", exist_ok=True)
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/rl_agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FlexChainRL')

# 设置命令行参数
parser = argparse.ArgumentParser(description='Rainbow for FlexChain Resource Optimization')
parser.add_argument('--id', type=str, default='flexchain_experiment', help='Experiment ID')
parser.add_argument('--seed', type=int, default=123, help='Random seed')
parser.add_argument('--disable-cuda', action='store_true', help='Disable CUDA')
parser.add_argument('--T-max', type=int, default=int(1e5), metavar='STEPS', help='Maximum number of training steps')
parser.add_argument('--max-episode-length', type=int, default=int(1e4), metavar='LENGTH', help='Max episode length (not strictly applicable)')
parser.add_argument('--history-length', type=int, default=1, metavar='T', help='Number of consecutive states processed')
parser.add_argument('--architecture', type=str, default='selfish_mining', choices=['canonical', 'data-efficient','selfish_mining'], metavar='ARCH', help='Network architecture')
parser.add_argument('--hidden-size', type=int, default=80, metavar='SIZE', help='Network hidden size')
parser.add_argument('--noisy-std', type=float, default=0.1, metavar='σ', help='Initial standard deviation of noisy linear layers')
parser.add_argument('--atoms', type=int, default=51, metavar='C', help='Discretised size of value distribution')
parser.add_argument('--V-min', type=float, default=-10, metavar='V', help='Minimum of value distribution support')
parser.add_argument('--V-max', type=float, default=10, metavar='V', help='Maximum of value distribution support')
parser.add_argument('--model', type=str, metavar='PARAMS', help='Pretrained model (state dict)')
parser.add_argument('--memory-capacity', type=int, default=int(5e4), metavar='CAPACITY', help='Experience replay memory capacity')
parser.add_argument('--replay-frequency', type=int, default=2, metavar='k', help='Frequency of sampling from memory')
parser.add_argument('--priority-exponent', type=float, default=0.5, metavar='ω', help='Prioritised experience replay exponent')
parser.add_argument('--priority-weight', type=float, default=0.4, metavar='β', help='Initial prioritised experience replay importance sampling weight')
parser.add_argument('--multi-step', type=int, default=3, metavar='n', help='Number of steps for multi-step return')
parser.add_argument('--discount', type=float, default=0.99, metavar='γ', help='Discount factor')
parser.add_argument('--target-update', type=int, default=int(8e3), metavar='τ', help='Number of steps after which to update target network')
parser.add_argument('--soft-target-update', type=int, default=int(10), metavar='τ', help='Number of steps after which to soft update target network')
parser.add_argument('--reward-clip', type=int, default=0, metavar='VALUE', help='Reward clipping (0 to disable)')
parser.add_argument('--learning-rate', type=float, default=0.001, metavar='η', help='Learning rate')
parser.add_argument('--adam-eps', type=float, default=1.5e-4, metavar='ε', help='Adam epsilon')
parser.add_argument('--batch-size', type=int, default=64, metavar='SIZE', help='Batch size')
parser.add_argument('--norm-clip', type=float, default=10, metavar='NORM', help='Max L2 norm for gradient clipping')
parser.add_argument('--learn-start', type=int, default=int(1e2), metavar='STEPS', help='Number of steps before starting training')
parser.add_argument('--evaluate', action='store_true', help='Evaluate only')
parser.add_argument('--evaluation-interval', type=int, default=10000, metavar='STEPS', help='Number of training steps between evaluations')
parser.add_argument('--evaluation-episodes', type=int, default=5, metavar='N', help='Number of evaluation episodes to average over')
parser.add_argument('--evaluation-size', type=int, default=5, metavar='N', help='Number of transitions to use for validating Q')
parser.add_argument('--render', action='store_true', help='Display screen (testing only)')
parser.add_argument('--enable-cudnn', action='store_true', help='Enable cuDNN (faster but nondeterministic)')
parser.add_argument('--checkpoint-interval', default=0, type=int, help='How often to checkpoint the model')
parser.add_argument('--memory', help='Path to save/load the memory from')
parser.add_argument('--disable-bzip-memory', action='store_true', help='Don\'t zip the memory file')
parser.add_argument('--server-port', type=int, default=50055, help='gRPC服务器端口')
parser.add_argument('--results-dir', type=str, default='results', help='Directory to store results')
parser.add_argument('--baseline-mode', action='store_true', help='if it is not rl but baseline')
parser.add_argument('--simulation-mode', action='store_true', help='Use simulation environment instead of real system')
parser.add_argument('--simulation-data-dir', type=str, default='processed_data', help='Directory containing CSV simulation data')

# 交替训练与评估的相关参数
parser.add_argument('--train-duration', type=int, default=360, help='Training phase duration in seconds (default: 20 mins)')
parser.add_argument('--eval-duration', type=int, default=160, help='Evaluation phase duration in seconds (default: 10mins   )')
parser.add_argument('--performance-threshold', type=float, default=1.05, help='Improvement threshold to save new best model (default: 5%)')
parser.add_argument('--degradation-threshold', type=float, default=0.9, help='Degradation threshold to rollback to best model (default: 10%)')

# 优化选项
parser.add_argument('--noisy', action='store_true', help='Use noisy net but not epsilon-greedy')
parser.add_argument('--prior-mem', action='store_true', help='Use prioritized replay memory but not random sampling')
parser.add_argument('--distri', action='store_true', help='Use distributed Q-learning')

# 日志设置
parser.add_argument('--log-file', type=str, default='logs/training.log', help='Log file path')

# Simple ISO 8601 timestamped logger
def log(filename, s):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'a') as f:
        f.write('[' + str(datetime.now().strftime('%Y-%m-%dT%H:%M:%S')) + '] ' + s + '\n')
    logger.info(s)

def load_memory(memory_path, disable_bzip):
    if disable_bzip:
        with open(memory_path, 'rb') as pickle_file:
            return pickle.load(pickle_file)
    else:
        with bz2.open(memory_path, 'rb') as zipped_pickle_file:
            return pickle.load(zipped_pickle_file)

def save_memory(memory, memory_path, disable_bzip):
    os.makedirs(os.path.dirname(memory_path), exist_ok=True)
    if disable_bzip:
        with open(memory_path, 'wb') as pickle_file:
            pickle.dump(memory, pickle_file)
    else:
        with bz2.open(memory_path, 'wb') as zipped_pickle_file:
            pickle.dump(memory, zipped_pickle_file)

def calculate_window_performance(performance_history, window_duration=3600):
    """计算最近时间窗口内的平均性能"""
    if not performance_history:
        return 0
    
    current_time = time.time()
    window_start = current_time - window_duration
    
    # 过滤出时间窗口内的性能记录
    window_records = [(t, perf) for t, perf in performance_history if t >= window_start]
    
    if not window_records:
        return 0
    
    # 计算平均性能
    avg_performance = sum(perf for _, perf in window_records) / len(window_records)
    return avg_performance

def main():
    # 解析参数
    args = parser.parse_args()
    
    # 打印参数
    print(' ' * 26 + 'Options')
    for k, v in vars(args).items():
        print(' ' * 26 + k + ': ' + str(v))
    
    # 创建结果目录
    results_dir = args.results_dir
    os.makedirs(results_dir, exist_ok=True)
    
    # 创建日志目录
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    
    # 如果没有提供内存文件路径，使用默认路径
    if not args.memory:
        args.memory = os.path.join(results_dir, 'memfile')
    
    # 初始化度量指标
    metrics = {
        'steps': [], 
        'rewards': [], 
        'Qs': [], 
        'best_avg_reward': -float('inf'),
        'performance_history': [],
        'episode_rewards': [],  # 新增：记录episode奖励
        'evaluation_scores': []  # 新增：记录评估分数
    }
    
    # 设置随机种子
    np.random.seed(args.seed)
    torch.manual_seed(np.random.randint(1, 10000))
    
    # 检测是否使用CUDA
    if torch.cuda.is_available() and not args.disable_cuda:
        args.device = torch.device('cuda')
        torch.cuda.manual_seed(np.random.randint(1, 10000))
        torch.backends.cudnn.enabled = args.enable_cudnn
    else:
        args.device = torch.device('cpu')
    
    # 创建环境
    if args.baseline_mode:
        mmm = True
        action = 13 #(0,0,0)
    else:
        mmm = False
    env = FlexChainRLEnv(
        server_port=args.server_port,
        baseline_mode=mmm,
        simulation_mode=args.simulation_mode,  # 从命令行参数读取
        simulation_data_dir=args.simulation_data_dir
    )
    
    # 获取动作空间大小
    action_space = env.action_space_n.n
    
    # 创建智能体
    dqn = Agent(args, env)
    
    # 如果提供了预训练模型，尝试加载
    if args.model is not None:
        if os.path.isfile(args.model):
            log(args.log_file, f'Loading model from {args.model}')
            state_dict = torch.load(args.model, map_location='cpu')
            
            # 处理旧模型兼容性问题
            if 'conv1.weight' in state_dict.keys():
                for old_key, new_key in (('conv1.weight', 'convs.0.weight'), ('conv1.bias', 'convs.0.bias'), 
                                ('conv2.weight', 'convs.2.weight'), ('conv2.bias', 'convs.2.bias'), 
                                ('conv3.weight', 'convs.4.weight'), ('conv3.bias', 'convs.4.bias')):
                    if old_key in state_dict:
                        state_dict[new_key] = state_dict[old_key]
                        del state_dict[old_key]
            
            dqn.online_net.load_state_dict(state_dict)
            log(args.log_file, 'Model loaded successfully')
        else:
            log(args.log_file, f'Warning: No model found at {args.model}')
    
    # 尝试加载内存
    try:
        mem = load_memory(args.memory, args.disable_bzip_memory)
        log(args.log_file, f'Memory loaded from {args.memory}')
    except (FileNotFoundError, EOFError):
        mem = createReplayMemory(args, args.memory_capacity)
        log(args.log_file, 'New memory initialized')
    
    # 计算priority_weight增加的量
    priority_weight_increase = (1 - args.priority_weight) / (args.T_max - args.learn_start)
    
    # 构建验证内存（用于Q值评估）
    val_mem = createReplayMemory(args, args.evaluation_size)
    T, done = 0, True
    
    # 填充验证内存
    log(args.log_file, 'Populating validation memory...')
    while T < args.evaluation_size:
        if done:
            state, _ = env.reset()
        
        action = np.random.randint(0, action_space)
        next_state, reward, done, truncated, _ = env.step(action)
        
        # 这里需要适配你的createReplayMemory接口
        val_mem.append(state, action, reward, done)
        
        state = next_state
        T += 1
    
    # 仅评估模式
    if args.evaluate:
        log(args.log_file, 'Starting evaluation mode...')
        dqn.eval()  # 设置DQN为评估模式
        avg_reward, avg_Q = test_during_training(args, env, 0, dqn, val_mem, metrics, results_dir, evaluate=True)
        log(args.log_file, f'Evaluation results - Avg. reward: {avg_reward} | Avg. Q: {avg_Q}')
        return
    
    # 主训练循环
    log(args.log_file, 'Starting training...')
    dqn.train()
    
    training_phase = True
    steps_in_current_phase = 0
    train_phase_length = args.train_duration  # 每2000步一个训练阶段
    eval_phase_length = args.eval_duration  # 每500步一个评估阶段
    
    best_model_state = None
    best_performance = -float('inf')
    current_episode_reward = 0
    episode_step_count = 0
    
    T = 0
    done = True
    
    pbar = tqdm(total=args.T_max)
    
    try:
        while T < args.T_max:
            # 修复3: 基于步数的阶段切换
            if training_phase and steps_in_current_phase >= train_phase_length:
                # 从训练切换到评估
                log(args.log_file, f"Step {T}: Switching to evaluation phase")
                training_phase = False
                steps_in_current_phase = 0
                dqn.eval()
                
                # 保存当前训练的模型用于评估
                current_model_for_eval = copy.deepcopy(dqn.online_net.state_dict())
                
            elif not training_phase and steps_in_current_phase >= eval_phase_length:
                # 从评估切换到训练
                log(args.log_file, f"Step {T}: Switching to training phase")
                
                # 修复4: 正确的性能评估
                if len(metrics['episode_rewards']) > 0:
                    recent_performance = np.mean(metrics['episode_rewards'][-10:])  # 最近10个episode的平均奖励
                    metrics['evaluation_scores'].append(recent_performance)
                    
                    # 修复5: 正确的模型保存逻辑
                    if recent_performance > best_performance:
                        improvement = (recent_performance - best_performance) if best_performance > -float('inf') else 0
                        log(args.log_file, f"New best performance: {recent_performance:.4f} (improvement: +{improvement:.4f})")
                        
                        best_performance = recent_performance
                        best_model_state = current_model_for_eval  # 保存正确的模型状态
                        
                        # 立即保存最佳模型
                        torch.save(best_model_state, os.path.join(results_dir, 'best_model.pth'))
                        
                    # 检查是否需要回滚（性能严重下降）
                    elif recent_performance < best_performance * 0.8 and best_model_state is not None:
                        degradation = (best_performance - recent_performance) / best_performance * 100
                        log(args.log_file, f"Performance degraded by {degradation:.1f}% - Rolling back to best model")
                        dqn.online_net.load_state_dict(best_model_state)
                
                training_phase = True
                steps_in_current_phase = 0
                dqn.train()
            
            # 重置环境（如果需要）
            if done:
                if current_episode_reward != 0:
                    metrics['episode_rewards'].append(current_episode_reward)
                    log(args.log_file, f"Episode finished: reward={current_episode_reward:.2f}, steps={episode_step_count}")
                
                state, _ = env.reset()
                current_episode_reward = 0
                episode_step_count = 0
            
            # 动作选择和执行
            if training_phase and T % args.replay_frequency == 0:
                dqn.reset_noise()
            
            action = dqn.act(state)
            if training_phase:
                dqn.update_epsilon()
            
            # 等待环境处理
            while not env.processing:
                time.sleep(0.1)
            
            next_state, reward, done, truncated, info = env.step(action)
            
            # 累积episode奖励
            current_episode_reward += reward
            episode_step_count += 1
            
            # 修复6: 在训练阶段才进行学习
            if training_phase:
                mem.append(state, action, reward, done)
                
                if T >= args.learn_start:
                    if args.prior_mem:
                        mem.priority_weight = min(mem.priority_weight + priority_weight_increase, 1)
                    
                    if T % args.replay_frequency == 0:
                        dqn.learn(mem)
                    
                    if T % args.soft_target_update == 0:
                        dqn.update_target_net()
            
            # 定期保存检查点
            if T % 1000 == 0:
                checkpoint_path = os.path.join(results_dir, f'checkpoint_{T}.pth')
                torch.save(dqn.online_net.state_dict(), checkpoint_path)
                save_memory(mem, args.memory, args.disable_bzip_memory)
                
                # 记录训练状态
                avg_reward = np.mean(metrics['episode_rewards'][-20:]) if len(metrics['episode_rewards']) >= 20 else 0
                log(args.log_file, f"Step {T}: Phase={'Train' if training_phase else 'Eval'}, "
                          f"Avg Reward (last 20): {avg_reward:.3f}, Best: {best_performance:.3f}")
            
            state = next_state
            T += 1
            steps_in_current_phase += 1
            pbar.update(1)
        
        # 训练结束，保存最终模型
        final_model_path = os.path.join(results_dir, 'final_model.pth')
        torch.save(dqn.online_net.state_dict(), final_model_path)
        
        # 如果最佳模型更好，复制为最终模型
        if best_model_state is not None and best_performance > np.mean(metrics['episode_rewards'][-10:]):
            torch.save(best_model_state, final_model_path)
            log(args.log_file, f"Best model (performance: {best_performance:.3f}) saved as final model")
        
        log(args.log_file, f'Training completed after {T} steps')
        
    except KeyboardInterrupt:
        log(args.log_file, f'Training interrupted after {T} steps')
        
        # 保存中断时的状态
        interrupted_path = os.path.join(results_dir, 'interrupted_model.pth')
        torch.save(dqn.online_net.state_dict(), interrupted_path)
        
        # 如果有最佳模型，也保存一份
        if best_model_state is not None:
            torch.save(best_model_state, os.path.join(results_dir, 'best_model_interrupted.pth'))
    
    finally:
        env.close()
        pbar.close()

if __name__ == '__main__':
    main()