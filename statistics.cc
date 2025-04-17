#include "statistics.h"

extern std::atomic<long> total_ops;
extern std::atomic<long> abort_count;
extern std::atomic<long> cache_hit;
extern std::atomic<long> cache_total;
extern std::atomic<long> sst_count;
extern volatile int end_flag;
static pthread_t stats_thread;

typedef struct {
    unsigned long long user, nice, system, idle, iowait, irq, softirq, steal;
} cpu_stats_t;

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

static void *statistics_thread(void *arg) {
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

        double mem_usage = get_memory_usage_percent();

        log_info(stderr,
            "[Total] ops=%ld, aborts=%ld, cache_hit=%ld/%ld, sst_count=%ld | "
            "[Interval] ops=%ld, cache_hit=%ld | Mem: %.2f%% | CPU: %.2f%%",
            current_ops, abort_count.load(), current_cache_hit, cache_total.load(), sst_count.load(),
            ops_diff, cache_hit_diff, mem_usage, cpu_usage);
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