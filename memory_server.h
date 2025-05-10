#ifndef MEMORY_SERVER_H
#define MEMORY_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <string.h>

#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include "storage.grpc.pb.h"
#include "memory_config.grpc.pb.h"

// 移除宏定义
// #define EVICT_THR 100
extern uint32_t EVICT_THR;
extern pthread_mutex_t evict_thr_mutex;

using namespace std;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// 添加新的MemoryConfigService服务类
class MemoryConfigServiceImpl final : public MemoryConfig::Service {
    public:
        Status SetEvictThreshold(ServerContext* context, const EvictThresholdRequest* request,
                                 EvictThresholdResponse* response) override;
        
        Status GetEvictThreshold(ServerContext* context, const GetEvictThresholdRequest* request,
                                 EvictThresholdResponse* response) override;
};
    
// 添加线程函数声明
void* config_service_thread(void* arg);

struct EntryHeader {
    uint64_t ptr_next;
    uint64_t bk_addr;
    bool in_memory;
    pthread_rwlock_t rwlock;
};

class RequestQueue {
   public:
    queue<struct ibv_wc> wc_queue;
    pthread_mutex_t mutex;
    sem_t full; //信号量，8字节对齐，union类型（共享同一块内存空间），long int _aligned用于骗编译器让它内存对齐

    RequestQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~RequestQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

class SpaceAllocator {
   public:
    queue<char *> free_addrs;
    sem_t full;
    pthread_mutex_t lock;
    pthread_cond_t cv_below;

    SpaceAllocator() {
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init(&cv_below, NULL);
    }

    ~SpaceAllocator() {
        pthread_mutex_destroy(&lock);
        pthread_cond_destroy(&cv_below);
    }
};

class GarbageCollector {
   public:
    struct GCItem {
        char *addr;
        string key;
    };
    queue<struct GCItem> to_gc_queue;
    pthread_mutex_t lock;
    pthread_cond_t cv_above;

    GarbageCollector() {
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init(&cv_above, NULL);
    }

    ~GarbageCollector() {
        pthread_mutex_destroy(&lock);
        pthread_cond_destroy(&cv_above);
    }
};

class KVStableClient {
   public:
    KVStableClient(std::shared_ptr<Channel> channel)
        : stub_(KVStable::NewStub(channel)) {}

    void write_sstables(const set<string>& keys);

   private:
    unique_ptr<KVStable::Stub> stub_;
};

#endif