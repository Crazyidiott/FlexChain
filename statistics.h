#ifndef STATISTICS_H
#define STATISTICS_H

#include <atomic>
#include <pthread.h>
#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include "log.h"
#include <memory>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <vector>
#include <deque>
#include <mutex>

#include "rl_agent.grpc.pb.h"
#include "memory_config.pb.h"
#include "memory_config.grpc.pb.h"

#define MAX_HISTORY_STATES 100

using grpc::Channel;
using grpc::ClientAsyncWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;


void start_statistics_thread();
void stop_statistics_thread();
void *statistics_thread(void *arg);
void *statistics_thread_avg(void *arg);

extern std::atomic<long> total_ops;
extern std::atomic<long> abort_count;
extern std::atomic<long> cache_hit;
extern std::atomic<long> cache_total;
extern std::atomic<long> sst_count;
extern std::atomic<long> YCSB_ops;
extern std::atomic<long> KMEANS_ops;
extern std::atomic<long> BANK_ops;
extern std::shared_ptr<grpc::Channel> memory_config_channel_ptr;
class CoreManager;
extern CoreManager* g_core_manager;
extern std::shared_ptr<grpc::Channel> rl_agent_channel_ptr;

typedef struct {
    unsigned long long user, nice, system, idle, iowait, irq, softirq, steal;
} cpu_stats_t;

class MemoryConfigClient {
    public:
        MemoryConfigClient(std::shared_ptr<Channel> channel)
            : stub_(MemoryConfig::NewStub(channel)) {}
    
        // 设置驱逐阈值
        bool SetEvictThreshold(uint32_t threshold, uint32_t& new_threshold);
        
        // 获取当前驱逐阈值
        bool GetEvictThreshold(uint32_t& threshold, uint32_t& free_addrs);
    
    private:
        std::unique_ptr<MemoryConfig::Stub> stub_;
};

// RL Agent客户端类
class RLAgentClient {
public:
    RLAgentClient(std::shared_ptr<Channel> channel)
        : stub_(RLAgent::NewStub(channel)) {}
    
    // 发送系统状态到RL Agent并获取配置
    bool SendSystemStates(const std::vector<SystemState>& states, SystemConfig& config);
    
    // 仅获取配置
    bool GetSystemConfig(SystemConfig& config);

private:
    std::unique_ptr<RLAgent::Stub> stub_;
};

// 系统状态管理类
class SystemStateManager {
public:
    SystemStateManager(int interval_seconds = 5, CoreManager* core_manager = nullptr);
    
    // 启动状态收集线程
    void StartCollection();
    
    // 停止状态收集线程
    void StopCollection();
    
    // 获取当前系统状态
    SystemState GetCurrentState();
    
    // 获取历史系统状态
    std::vector<SystemState> GetHistoryStates(int count = MAX_HISTORY_STATES);
    
    // 发送状态到RL agent并应用配置
    bool SendStatesAndApplyConfig();

private:
    // 状态收集线程函数
    static void* CollectionThread(void* arg);
    
    // 计算内存利用率
    double CalculateMemoryUtilization(uint32_t free_addrs, uint32_t evict_thr);

    double SystemStateManager::CalculateCpuUtilization(int cpu_id);
    
    // 计算平均CPU利用率
    double CalculateAverageCpuUtilization();
    
    // 应用从RL agent接收的配置
    void ApplyConfig(const SystemConfig& config);
    
    std::deque<SystemState> state_history_;  // 状态历史
    std::mutex state_mutex_;                 // 保护状态历史的互斥锁
    int interval_seconds_;                   // 收集间隔(秒)
    pthread_t collection_thread_;            // 收集线程ID
    volatile bool running_;                  // 线程运行标志
    CoreManager* core_manager_;              // CoreManager引用
    
    // 保存上一次采集的操作计数，用于计算interval内的增量
    long last_ycsb_ops_;
    long last_kmeans_ops_;
    long last_bank_ops_;
    long last_total_ops_;
    
    // CPU利用率计算相关
    std::vector<cpu_stats_t> last_cpu_stats_by_core_;
};
extern SystemStateManager* g_state_manager;



#endif // STATISTICS_H
