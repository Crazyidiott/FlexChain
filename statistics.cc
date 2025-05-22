#include "statistics.h"
#include "compute_server.h"
extern std::atomic<long> total_ops;
extern std::atomic<long> abort_count;
extern std::atomic<long> cache_hit;
extern std::atomic<long> cache_total;
extern std::atomic<long> sst_count;
extern volatile int end_flag;
extern std::atomic<long> YCSB_ops;
extern std::atomic<long> KMEANS_ops;
extern std::atomic<long> BANK_ops;
pthread_t stats_thread;
SystemStateManager* g_state_manager = nullptr;
extern std::shared_ptr<grpc::Channel> rl_agent_channel_ptr;
extern CoreManager* g_core_manager; 

#define MAX_CPUS 128   // 假设不超过128个核心


bool MemoryConfigClient::SetEvictThreshold(uint32_t threshold, uint32_t& new_threshold) {
    ClientContext context;
    EvictThresholdRequest request;
    EvictThresholdResponse response;
    
    request.set_threshold(threshold);
    Status status = stub_->SetEvictThreshold(&context, request, &response);
    
    if (status.ok() && response.success()) {
        new_threshold = response.threshold();
        return true;
    }
    return false;
}

bool MemoryConfigClient::GetEvictThreshold(uint32_t& threshold, uint32_t& free_addrs) {
    ClientContext context;
    GetEvictThresholdRequest request;
    EvictThresholdResponse response;
    
    Status status = stub_->GetEvictThreshold(&context, request, &response);
    
    if (status.ok() && response.success()) {
        threshold = response.threshold();
        free_addrs = response.free_addrs();
        return true;
    }
    return false;
}


// 获取CPU核心数
int get_cpu_count() {
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) return -1;

    char line[256];
    int count = 0;
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "cpu", 3) == 0 && isdigit((unsigned char)line[3]))
            count++;
    }

    fclose(fp);
    return count;
}

// 获取所有cpu核的状态
static int read_all_cpu_stats(cpu_stats_t *stats_array, int cpu_count) {
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) {
        perror("Failed to open /proc/stat");
        return -1;
    }

    char line[256];
    int index = 0;

    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "cpu", 3) == 0 && isdigit(line[3])) {
            if (index >= cpu_count) break;

            sscanf(line, "cpu%*d %llu %llu %llu %llu %llu %llu %llu %llu",
                   &stats_array[index].user,
                   &stats_array[index].nice,
                   &stats_array[index].system,
                   &stats_array[index].idle,
                   &stats_array[index].iowait,
                   &stats_array[index].irq,
                   &stats_array[index].softirq,
                   &stats_array[index].steal);
            index++;
        }
    }

    fclose(fp);
    return 0;
}

bool read_all_cpu_stats(std::vector<cpu_stats_t>& stats_array) {
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) {
        log_err("Failed to open /proc/stat");
        return false;
    }

    char line[256];
    int index = 0;
    
    // 读取第一行(总体CPU)并跳过
    if (fgets(line, sizeof(line), fp) == NULL) {
        fclose(fp);
        return false;
    }
    
    // 读取每个CPU核心的数据
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "cpu", 3) == 0 && isdigit(line[3])) {
            cpu_stats_t stats;
            sscanf(line, "cpu%*d %llu %llu %llu %llu %llu %llu %llu %llu",
                   &stats.user,
                   &stats.nice,
                   &stats.system,
                   &stats.idle,
                   &stats.iowait,
                   &stats.irq,
                   &stats.softirq,
                   &stats.steal);
            
            // 如果索引超出数组大小，扩展数组
            if (index >= stats_array.size()) {
                stats_array.push_back(stats);
            } else {
                stats_array[index] = stats;
            }
            index++;
        }
    }

    fclose(fp);
    return true;
}

// 获取本机器某一个时刻的cpu平均状态
static int read_cpu_stats(cpu_stats_t *stats) {
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) {
        log_err("Failed to open /proc/stat");
        return -1;
    }

    fscanf(fp, "cpu  %llu %llu %llu %llu %llu %llu %llu %llu",
           &stats->user, &stats->nice, &stats->system, &stats->idle,
           &stats->iowait, &stats->irq, &stats->softirq, &stats->steal);
    fclose(fp);
    return 0;
}

// 计算CPU使用率
static double compute_cpu_usage(cpu_stats_t *prev, cpu_stats_t *curr) {
    unsigned long long prev_idle = prev->idle + prev->iowait;
    unsigned long long curr_idle = curr->idle + curr->iowait;

    unsigned long long prev_non_idle = prev->user + prev->nice + prev->system + prev->irq + prev->softirq + prev->steal;
    unsigned long long curr_non_idle = curr->user + curr->nice + curr->system + curr->irq + curr->softirq + curr->steal;

    unsigned long long prev_total = prev_idle + prev_non_idle;
    unsigned long long curr_total = curr_idle + curr_non_idle;

    double totald = (double)(curr_total - prev_total);
    double idled = (double)(curr_idle - prev_idle);

    return ((totald - idled) / totald) * 100.0;
}

double compute_cpu_usage(const cpu_stats_t& prev, const cpu_stats_t& curr) {
    unsigned long long prev_idle = prev.idle + prev.iowait;
    unsigned long long curr_idle = curr.idle + curr.iowait;

    unsigned long long prev_non_idle = prev.user + prev.nice + prev.system + prev.irq + prev.softirq + prev.steal;
    unsigned long long curr_non_idle = curr.user + curr.nice + curr.system + curr.irq + curr.softirq + curr.steal;

    unsigned long long prev_total = prev_idle + prev_non_idle;
    unsigned long long curr_total = curr_idle + curr_non_idle;

    // 防止除以0
    if (curr_total == prev_total) {
        return 0.0;
    }

    double totald = (double)(curr_total - prev_total);
    double idled = (double)(curr_idle - prev_idle);

    return ((totald - idled) / totald) * 100.0;
}


// 计算区间指标, 获取每个cpu的usage数据
void *statistics_thread_old(void *arg) {
    long last_ops = total_ops.load();
    long last_cache_hit = cache_hit.load();

    int cpu_count = get_cpu_count();
    if (cpu_count <= 0) {
        fprintf(stderr, "Failed to determine CPU core count.\n");
        return NULL;
    }

    cpu_stats_t prev_stats[MAX_CPUS], curr_stats[MAX_CPUS];
    memset(prev_stats, 0, sizeof(prev_stats));
    memset(curr_stats, 0, sizeof(curr_stats));

    // 初次采样
    read_all_cpu_stats(prev_stats, cpu_count);

    while (!end_flag) {
        long wait_seconds = 5;
        sleep(wait_seconds);

        long current_ops = total_ops.load();
        long current_cache_hit = cache_hit.load();
        long ops_diff = current_ops - last_ops;
        long cache_hit_diff = current_cache_hit - last_cache_hit;

        last_ops = current_ops;
        last_cache_hit = current_cache_hit;

        read_all_cpu_stats(curr_stats, cpu_count);
    
        for (int i = 0; i < cpu_count; i++) {
            double usage = compute_cpu_usage(&prev_stats[i], &curr_stats[i]);
            // printf("CPU Core %d Usage: %.2f%%\n", i, usage);
            prev_stats[i] = curr_stats[i];
        }        

        // log all info
        // log_info(stderr,
        //         "[Total] ops=%ld, aborts=%ld, cache_hit=%ld/%ld, sst_count=%ld | "
        //         "[Interval] ops=%ld, cache_hit=%ld",
        //         current_ops, abort_count.load(), current_cache_hit, cache_total.load(), sst_count.load(),
        //         ops_diff, cache_hit_diff);

        MemoryConfigClient config_client(memory_config_channel_ptr);
    
        // 获取当前阈值
        uint32_t current_threshold;
        uint32_t current_free_addrs;
        if (config_client.GetEvictThreshold(current_threshold, current_free_addrs)) {
            log_info(stderr, "Current EVICT_THR: %u, Current free addrs: %u", current_threshold, current_free_addrs);
        }
        
        // 设置新阈值
        uint32_t new_threshold;
        if (config_client.SetEvictThreshold(200, new_threshold)) {
            log_info(stderr, "Updated EVICT_THR to: %u", new_threshold);
        }

        log_info(stderr,
            "[Interval] ops=%ld, cache_hit=%ld, ops/s=%f",
            ops_diff, cache_hit_diff, ops_diff * 1.0 / wait_seconds);

    }
    return NULL;

}
// 绑定当前线程到指定CPU
void bind_thread_to_cpu(int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    pthread_t thread = pthread_self();  // 当前线程
    if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np failed");
    }
}

// 计算区间指标

void *statistics_thread_avg(void *arg) {
    long last_ops = total_ops.load();
    long last_cache_hit = cache_hit.load();

    cpu_stats_t prev_stats, curr_stats;
    read_cpu_stats(&prev_stats);

    while (!end_flag) {
        sleep(5);

        long current_ops = total_ops.load();
        long current_cache_hit = cache_hit.load();
        long ops_diff = current_ops - last_ops;
        long cache_hit_diff = current_cache_hit - last_cache_hit;

        last_ops = current_ops;
        last_cache_hit = current_cache_hit;

        read_cpu_stats(&curr_stats);
        double cpu_usage = compute_cpu_usage(&prev_stats, &curr_stats);
        prev_stats = curr_stats;

        log_info(stderr,
                "[Total] ops=%ld, aborts=%ld, cache_hit=%ld/%ld, sst_count=%ld | "
                "[Interval] ops=%ld, cache_hit=%ld | CPU: %.2f%%",
                current_ops, abort_count.load(), current_cache_hit, cache_total.load(), sst_count.load(),
                ops_diff, cache_hit_diff, cpu_usage);

        // double mem_usage = get_memory_usage_percent();

        // log_info(stderr,
        //     "[Total] ops=%ld, aborts=%ld, cache_hit=%ld/%ld, sst_count=%ld | "
        //     "[Interval] ops=%ld, cache_hit=%ld | Mem: %.2f%% | CPU: %.2f%%",
        //     current_ops, abort_count.load(), current_cache_hit, cache_total.load(), sst_count.load(),
        //     ops_diff, cache_hit_diff, mem_usage, cpu_usage);
    }
    return NULL;
}

// RLAgentClient实现
bool RLAgentClient::SendSystemStates(const std::vector<SystemState>& states, SystemConfig& config) {
    ClientContext context;
    SystemStatesRequest request;
    
    // 添加所有状态到请求
    for (const auto& state : states) {
        *request.add_states() = state;
    }
    
    // 添加节点ID (可选)
    request.set_node_id("compute_node_1");
    
    // 发送请求
    Status status = stub_->SendSystemStates(&context, request, &config);
    return status.ok();
}

bool RLAgentClient::GetSystemConfig(SystemConfig& config) {
    ClientContext context;
    ConfigRequest request;
    
    // 添加节点ID (可选)
    request.set_node_id("compute_node_1");
    
    // 发送请求
    Status status = stub_->GetSystemConfig(&context, request, &config);
    return status.ok();
}

// SystemStateManager实现
SystemStateManager::SystemStateManager(int interval_milliseconds, CoreManager* core_manager)
    : interval_milliseconds(interval_milliseconds),
      running_(false),
      core_manager_(core_manager),
      last_ycsb_ops_(0),
      last_kmeans_ops_(0),
      last_bank_ops_(0),
      last_total_ops_(0) {
    
    // 初始化所有CPU核心的统计数据
    // 假设最多有128个CPU核心
    last_cpu_stats_by_core_.resize(128);
    read_all_cpu_stats(last_cpu_stats_by_core_);
}

void SystemStateManager::StartCollection() {
    if (running_) return;
    
    running_ = true;
    // 创建状态收集线程
    pthread_create(&collection_thread_, NULL, CollectionThread, this);
    log_info(stderr, "System state collection thread started");
}

void SystemStateManager::StopCollection() {
    if (!running_) return;
    
    running_ = false;
    pthread_join(collection_thread_, NULL);
    log_info(stderr, "System state collection thread stopped");
}

SystemState SystemStateManager::GetCurrentState() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (state_history_.empty()) {
        // 如果没有历史状态，创建一个默认状态
        return SystemState();
    }
    return state_history_.front();
}

std::vector<SystemState> SystemStateManager::GetHistoryStates(int count) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    std::vector<SystemState> result;
    
    // 取最近的count个状态，或全部状态如果少于count
    int actual_count = std::min(count, static_cast<int>(state_history_.size()));
    for (int i = 0; i < actual_count; i++) {
        result.push_back(state_history_[i]);
    }
    
    return result;
}

bool SystemStateManager::SendStatesAndApplyConfig() {
    // 获取历史状态
    std::vector<SystemState> states = GetHistoryStates(1);
    if (states.empty()) {
        log_info(stderr, "No states to send to RL agent");
        return false;
    }
    
    // 创建RL Agent客户端
    RLAgentClient client(rl_agent_channel_ptr);
    
    // 发送状态并获取配置
    SystemConfig config;
    if (!client.SendSystemStates(states, config)) {
        log_err("Failed to send system states to RL agent");
        return false;
    }
    
    // 应用配置
    ApplyConfig(config);
    
    return true;
}

void* SystemStateManager::CollectionThread(void* arg) {
    SystemStateManager* manager = static_cast<SystemStateManager*>(arg);
    
    while (manager->running_) {
        // 睡眠指定间隔
        // usleep(1000 * manager->interval_milliseconds);
        sleep(1);
        
        // 创建新的系统状态
        SystemState state;
        
        // 设置时间戳
        state.set_timestamp(time(NULL));
        
        // 获取当前操作计数
        long current_ycsb_ops = YCSB_ops.load();
        long current_kmeans_ops = KMEANS_ops.load();
        long current_bank_ops = BANK_ops.load();
        long current_total_ops = total_ops.load();
        
        // 计算interval内的操作数, 以秒为单位
        int interval_time = 1;
        long interval_ycsb_ops =  (current_ycsb_ops - manager->last_ycsb_ops_)/interval_time;
        long interval_kmeans_ops =  (current_kmeans_ops - manager->last_kmeans_ops_)/interval_time;
        long interval_bank_ops =  (current_bank_ops - manager->last_bank_ops_)/interval_time;
        long interval_total_ops = (current_total_ops - manager->last_total_ops_)/interval_time;
 
        state.set_ycsb_ops(interval_ycsb_ops);
        state.set_kmeans_ops(interval_kmeans_ops);
        state.set_bank_ops(interval_bank_ops);
        state.set_total_ops(interval_total_ops);
    
        
        // 更新上次计数
        manager->last_ycsb_ops_ = current_ycsb_ops;
        manager->last_kmeans_ops_ = current_kmeans_ops;
        manager->last_bank_ops_ = current_bank_ops;
        manager->last_total_ops_ = current_total_ops;
        
        // 获取CPU利用率
        state.set_cpu_utilization(manager->CalculateAverageCpuUtilization());
        
        // 获取内存使用情况
        MemoryConfigClient memory_client(memory_config_channel_ptr);
        uint32_t evict_thr, free_addrs;
        if (memory_client.GetEvictThreshold(evict_thr, free_addrs)) {
            log_info(stderr, "Current EVICT_THR: %u, Current free addrs: %u", evict_thr, free_addrs);
            state.set_evict_threshold(evict_thr);
            state.set_memory_utilization(manager->CalculateMemoryUtilization(free_addrs, evict_thr));
        } else {
            log_err("Failed to get memory information from memory server");
            state.set_evict_threshold(0);
            state.set_memory_utilization(0.0);
        }
        
        // 获取核心和线程信息
        if (manager->core_manager_) {
            state.set_core_count(manager->core_manager_->get_core_count());
            auto threads_per_core = manager->core_manager_->get_threads_per_core();
            state.set_sim_threads_per_core(threads_per_core.first);  // 模拟线程数
        } else {
            state.set_core_count(0);
            state.set_sim_threads_per_core(0);
        }
        
        // 保存状态到历史
        {
            std::lock_guard<std::mutex> lock(manager->state_mutex_);
            manager->state_history_.push_front(state);
            // 限制历史大小
            if (manager->state_history_.size() > MAX_HISTORY_STATES) {
                manager->state_history_.pop_back();
            }
        }
        
        // 发送状态到RL agent并应用配置
        // 注意：这里可能想要控制发送频率，不一定每次收集都发送
        // 发送的代码
        static int send_counter = 0;
        if (++send_counter >= 1) {  // 例如每3次收集发送一次
            // log_info(stderr,"fine here");
            manager->SendStatesAndApplyConfig();
            // log_info(stderr,"get through here");
            send_counter = 0;
        }
        
        // 记录日志
        log_info(stderr,
                "[State] interval_ops: YCSB=%d, KMEANS=%d, BANK=%d, TOTAL=%d |\n "
                "CPU: %.2f%%, Memory: %.2f%%, Cores: %d, Threads/Core: %d, EVICT_THR: %d\n"
                "cache_hit: %ld, sst_cnt: %ld, total_ops: %ld",
                state.ycsb_ops(), state.kmeans_ops(), state.bank_ops(), state.total_ops(),
                state.cpu_utilization(), state.memory_utilization(), 
                state.core_count(), state.sim_threads_per_core(), state.evict_threshold(),
                cache_hit.load(), sst_count.load(),total_ops.load());
    }
    
    return NULL;
}

double SystemStateManager::CalculateMemoryUtilization(uint32_t free_addrs, uint32_t evict_thr) {
    // 根据需求计算内存利用率: (400000 - free_addrs.size)/(400000 - evict_thr)
    const int TOTAL_MEMORY = 396000; // 总内存大小
    
    if (TOTAL_MEMORY <= evict_thr) {
        return 1.0; // 防止除以零
    }
    
    return static_cast<double>(TOTAL_MEMORY - free_addrs) / (TOTAL_MEMORY - evict_thr);
}

double SystemStateManager::CalculateCpuUtilization(int cpu_id) {
    if (cpu_id < 0 || cpu_id >= last_cpu_stats_by_core_.size()) {
        log_err("Invalid CPU ID: %d", cpu_id);
        return 0.0;
    }
    
    // 读取当前所有CPU统计
    std::vector<cpu_stats_t> current_stats_by_core = last_cpu_stats_by_core_;
    read_all_cpu_stats(current_stats_by_core);
    
    // 计算指定CPU的利用率
    double usage = compute_cpu_usage(last_cpu_stats_by_core_[cpu_id], current_stats_by_core[cpu_id]);
    
    // 更新该CPU的上次统计
    last_cpu_stats_by_core_[cpu_id] = current_stats_by_core[cpu_id];
    
    return usage;
}

double SystemStateManager::CalculateAverageCpuUtilization() {
    if (!core_manager_) {
        log_err("CoreManager is not available");
        return 0.0;
    }
    
    // 获取active cores列表
    std::vector<int> active_cores = core_manager_->GetActiveCores();
    
    if (active_cores.empty()) {
        log_err("No active cores found");
        return 0.0;
    }
    
    // 读取当前所有CPU统计
    std::vector<cpu_stats_t> current_stats_by_core = last_cpu_stats_by_core_;
    read_all_cpu_stats(current_stats_by_core);
    
    // 计算活动核心的平均利用率
    double total_usage = 0.0;
    int count = 0;
    
    for (int core_id : active_cores) {
        if (core_id < 0 || core_id >= last_cpu_stats_by_core_.size()) {
            log_warn("Invalid CPU ID in active_cores: %d", core_id);
            continue;
        }
        
        double usage = compute_cpu_usage(last_cpu_stats_by_core_[core_id], current_stats_by_core[core_id]);
        total_usage += usage;
        count++;
        
        // 更新该CPU的上次统计
        last_cpu_stats_by_core_[core_id] = current_stats_by_core[core_id];
    }
    
    if (count == 0) {
        return 0.0;
    }
    
    return total_usage / count;
}

void SystemStateManager::ApplyConfig(const SystemConfig& config) {
    log_info(stderr, "Applying config: core_adj=%d, thread_adj=%d, evict_thr_adj=%d",
             config.core_adjustment(), config.thread_adjustment(), config.evict_thr_adjustment());
    
    // 应用核心变动
    if (config.core_adjustment() != 0 && core_manager_) {
        if (config.core_adjustment() > 0) {
            // 添加核心
            for (int i = 0; i < config.core_adjustment(); i++) {
                int current_cores = core_manager_->get_core_count();
                core_manager_->add_core(current_cores); // 假设核心ID是连续的
            }
        } else if (config.core_adjustment() < 0) {
            // 移除核心
            for (int i = 0; i < -config.core_adjustment(); i++) {
                core_manager_->remove_core(); // 移除最后一个核心
            }
        }
    }
    
    // 应用线程变动
    if (config.thread_adjustment() != 0 && core_manager_) {
        core_manager_->adjust_thread(config.thread_adjustment(), 0);
    }
    
    // 应用evict_thr变动
    if (config.evict_thr_adjustment() != 0) {
        MemoryConfigClient memory_client(memory_config_channel_ptr);
        uint32_t current_thr, free_addrs;
        
        if (memory_client.GetEvictThreshold(current_thr, free_addrs)) {
            uint32_t new_thr = current_thr + config.evict_thr_adjustment();
            uint32_t actual_new_thr;
            
            if (memory_client.SetEvictThreshold(new_thr, actual_new_thr)) {
                log_info(stderr, "Updated EVICT_THR from %u to %u", current_thr, actual_new_thr);
            } else {
                log_err("Failed to update EVICT_THR");
            }
        } else {
            log_err("Failed to get current EVICT_THR");
        }
    }
}

// 修改statistics_thread函数，让它使用SystemStateManager
void *statistics_thread(void *arg) {
    // 获取传入的CoreManager实例
    CoreManager* core_manager = static_cast<CoreManager*>(arg);
    
    // 如果没有提供CoreManager，使用全局实例
    if (!core_manager) {
        core_manager = g_core_manager;
    }
    
    // 初始化SystemStateManager
    g_state_manager = new SystemStateManager(5, core_manager); // 5秒间隔
    g_state_manager->StartCollection();
    
    // 等待结束信号
    while (!end_flag) {
        sleep(1);
    }
    
    // 停止收集
    g_state_manager->StopCollection();
    delete g_state_manager;
    g_state_manager = nullptr;
    
    return NULL;
}

// 初始化RLAgent通信通道
void init_rl_agent_communication(const std::string& rl_agent_address) {
    // 创建到RL Agent的通道
    rl_agent_channel_ptr = grpc::CreateChannel(rl_agent_address, grpc::InsecureChannelCredentials());
    
    log_info(stderr, "Initialized RL Agent communication channel to %s", rl_agent_address.c_str());
}