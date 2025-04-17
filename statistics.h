#ifndef STATISTICS_H
#define STATISTICS_H

#include <atomic>
#include <pthread.h>
#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "log.h"

void start_statistics_thread();
void stop_statistics_thread();

extern std::atomic<long> total_ops;
extern std::atomic<long> abort_count;
extern std::atomic<long> cache_hit;
extern std::atomic<long> cache_total;
extern std::atomic<long> sst_count;


#endif // STATISTICS_H
