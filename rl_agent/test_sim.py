# test_sim_env.py
import logging
logging.basicConfig(level=logging.INFO)

from rl_env import FlexChainRLEnv

print("创建环境...")
env = FlexChainRLEnv(
    simulation_mode=True,
    simulation_data_dir='processed_data'
)

print("重置环境...")
obs, info = env.reset()
print(f"重置完成，观察值: {obs}")

print("执行一步...")
action = 0  # 不做任何调整
obs, reward, done, truncated, info = env.step(action)
print(f"步进完成，奖励: {reward}")

print("测试完成！")
env.close()
