#include "statistics.h"
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

#define MAX_CPUS 128   // 假设不超过128个核心

typedef struct {
    unsigned long long user, nice, system, idle, iowait, irq, softirq, steal;
} cpu_stats_t;


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

// 获取内存使用率，暂时不使用
static double get_memory_usage_percent() {
    FILE *fp = fopen("/proc/meminfo", "r");
    if (!fp) {
        log_err("Failed to open /proc/meminfo");
        return -1;
    }

    long mem_total = 0, mem_free = 0, buffers = 0, cached = 0;
    char key[64];
    long value;
    while (fscanf(fp, "%63s %ld", key, &value) == 2) {
        if (strcmp(key, "MemTotal:") == 0) mem_total = value;
        else if (strcmp(key, "MemFree:") == 0) mem_free = value;
        else if (strcmp(key, "Buffers:") == 0) buffers = value;
        else if (strcmp(key, "Cached:") == 0) cached = value;
    }
    fclose(fp);

    long used = mem_total - mem_free - buffers - cached;
    return (double)used / mem_total * 100.0;
}

// 计算区间指标, 获取每个cpu的usage数据
void *statistics_thread(void *arg) {
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

void start_statistics_thread() {
    if (pthread_create(&stats_thread, NULL, statistics_thread, NULL) != 0) {
        log_err("Failed to create statistics thread");
    } else {
        log_info(stderr, "Statistics thread started");
    }
}

void stop_statistics_thread() {
    pthread_join(stats_thread, NULL);
    log_info(stderr, "Statistics thread stopped");
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

// //old version
// void *statistics_thread(void *arg){
//     // log_info(stderr,"get into statistics thread");
//     while(!end_flag){
//         // log_info(stderr,"get into statistics thread's whileeee");
//         sleep(5);
//         log_info(stderr, "total_ops = %ld, abort_count = %ld, cache_hit = %ld, cache_total = %ld, sst_count = %ld",
//                 total_ops.load(), abort_count.load(), cache_hit.load(), cache_total.load(), sst_count.load());
//     }
//     return NULL;
// }

//how to use read all cpu stats
// int main() {
//     int cpu_count = get_cpu_count();
//     if (cpu_count <= 0) {
//         fprintf(stderr, "Failed to determine CPU core count.\n");
//         return 1;
//     }

//     cpu_stats_t prev_stats[MAX_CPUS], curr_stats[MAX_CPUS];
//     memset(prev_stats, 0, sizeof(prev_stats));
//     memset(curr_stats, 0, sizeof(curr_stats));

//     // 初次采样
//     read_all_cpu_stats(prev_stats, cpu_count);
//     sleep(1); // 等待一段时间再采样
//     read_all_cpu_stats(curr_stats, cpu_count);

//     for (int i = 0; i < cpu_count; i++) {
//         double usage = compute_cpu_usage(&prev_stats[i], &curr_stats[i]);
//         printf("CPU Core %d Usage: %.2f%%\n", i, usage);
//     }

//     return 0;
// }


