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
