# -*- coding: utf-8 -*-
from __future__ import division
import os
import torch
import plotly
from plotly.graph_objs import Scatter
from plotly.graph_objs.scatter import Line
import numpy as np
import time
import logging

logger = logging.getLogger('FlexChainRL.Test')

# Test during training function
def test_during_training(args, env, T, dqn, val_mem, metrics, results_dir, evaluate=False):
    """
    在训练过程中进行测试/评估
    不重置环境，只使用当前环境状态
    """
    # 记录当前步数
    if not evaluate:
        metrics['steps'].append(T)
    
    # 初始化评估指标收集器
    rewards = []
    Qs = []
    
    # 记录测试开始时间
    start_time = time.time()
    test_duration = 550  # 默认测试时间为550秒
    
    state, _ = env.reset()  # 获取当前环境状态
    total_reward = 0
    
    # 测试一段时间，而不是固定次数
    step_count = 0
    while time.time() - start_time < test_duration:
        # 使用ε-greedy策略选择动作
        action = dqn.act_e_greedy(state)
        if args.baseline_mode:
            action = 13  # 如果是基线模式，选择固定动作
        
        # 执行动作
        next_state, reward, done, truncated, _ = env.step(action)
        total_reward += reward
        
        # 更新状态
        state = next_state
        step_count += 1
        
        if done or truncated:
            break
    
    # 记录总奖励
    rewards.append(total_reward)
    
    # 计算平均Q值
    for state in val_mem:
        Qs.append(dqn.evaluate_q(state))
    
    # 计算平均指标
    avg_reward = total_reward / step_count if step_count > 0 else 0
    avg_Q = sum(Qs) / len(Qs) if Qs else 0
    
    if not evaluate:
        # 更新和保存指标
        metrics['rewards'].append(rewards)
        metrics['Qs'].append(Qs)
        torch.save(metrics, os.path.join(results_dir, 'metrics.pth'))
        
        # 绘制结果图表
        _plot_line(metrics['steps'], metrics['rewards'], 'Reward', path=results_dir)
        _plot_line(metrics['steps'], metrics['Qs'], 'Q', path=results_dir)
    
    # 记录评估结果
    logger.info(f"Test results - Steps: {step_count}, Avg reward: {avg_reward:.4f}, Avg Q: {avg_Q:.4f}")
    
    return avg_reward, avg_Q

# Plots min, max and mean + standard deviation bars of a population over time
def _plot_line(xs, ys_population, title, path=''):
    max_colour, mean_colour, std_colour, transparent = 'rgb(0, 132, 180)', 'rgb(0, 172, 237)', 'rgba(29, 202, 255, 0.2)', 'rgba(0, 0, 0, 0)'

    ys = torch.tensor(ys_population, dtype=torch.float32)
    ys_min, ys_max, ys_mean, ys_std = ys.min(1)[0].squeeze(), ys.max(1)[0].squeeze(), ys.mean(1).squeeze(), ys.std(1).squeeze()
    ys_upper, ys_lower = ys_mean + ys_std, ys_mean - ys_std

    trace_max = Scatter(x=xs, y=ys_max.numpy(), line=Line(color=max_colour, dash='dash'), name='Max')
    trace_upper = Scatter(x=xs, y=ys_upper.numpy(), line=Line(color=transparent), name='+1 Std. Dev.', showlegend=False)
    trace_mean = Scatter(x=xs, y=ys_mean.numpy(), fill='tonexty', fillcolor=std_colour, line=Line(color=mean_colour), name='Mean')
    trace_lower = Scatter(x=xs, y=ys_lower.numpy(), fill='tonexty', fillcolor=std_colour, line=Line(color=transparent), name='-1 Std. Dev.', showlegend=False)
    trace_min = Scatter(x=xs, y=ys_min.numpy(), line=Line(color=max_colour, dash='dash'), name='Min')

    plotly.offline.plot({
        'data': [trace_upper, trace_mean, trace_lower, trace_min, trace_max],
        'layout': dict(title=title, xaxis={'title': 'Step'}, yaxis={'title': title})
    }, filename=os.path.join(path, title + '.html'), auto_open=False)