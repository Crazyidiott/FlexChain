#ifndef COMPUTE_SERVER_H
#define COMPUTE_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "blockchain.grpc.pb.h"
#include "storage.grpc.pb.h"


using namespace std;
using grpc::Channel;
using grpc::ClientAsyncWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;

struct ThreadContext {
    int thread_index;
    volatile int end_flag;
    struct ibv_qp* m_qp;
    struct ibv_cq* m_cq;
};

class DataCache {
   public:
    struct Frame {
        uint64_t l_addr;
        uint64_t r_addr;
    };
    unordered_map<uint64_t, list<struct Frame>::iterator> addr_hashtable;
    pthread_mutex_t hashtable_lock;

    list<struct Frame> lru_list;

    queue<uint64_t> free_addrs;
    // since we use write-through, no need to use a background thread for flushing pages

    DataCache() {
        pthread_mutex_init(&hashtable_lock, NULL);
    }
    ~DataCache() {
        pthread_mutex_destroy(&hashtable_lock);
    }
};

class MetaDataCache {
   public:
    struct EntryHeader {
        uint64_t ptr_next;
        uint64_t bk_addr;
        string key;
    };
    unordered_map<string, list<struct EntryHeader>::iterator> key_hashtable;
    pthread_mutex_t hashtable_lock;

    list<struct EntryHeader> lru_list;

    // assume no space limit for metadata cache for now

    MetaDataCache() {
        pthread_mutex_init(&hashtable_lock, NULL);
    }
    ~MetaDataCache() {
        pthread_mutex_destroy(&hashtable_lock);
    }
};


class KVStableClient {
   public:
    KVStableClient(std::shared_ptr<Channel> channel)
        : stub_(KVStable::NewStub(channel)) {}

    int read_sstables(const string& key, string& value);
    int write_sstables(const string& key, const string& value);
    int write_blocks(const string& block);

   private:
    unique_ptr<KVStable::Stub> stub_;
};

class ComputeCommClient {
   public:
    ComputeCommClient(std::shared_ptr<Channel> channel)
        : stub_(ComputeComm::NewStub(channel)) {}

    int invalidate_cn(const string& key);
    void start_benchmarking();

   private:
    unique_ptr<ComputeComm::Stub> stub_;
};

class BlockQueue {
   public:
    queue<Block> bq_queue;
    pthread_mutex_t mutex;
    sem_t full;

    BlockQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~BlockQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

class ValidationQueue {
   public:
    queue<uint64_t> id_queue;
    queue<Endorsement> trans_queue;
    uint64_t curr_block_id;
    pthread_mutex_t mutex;
    sem_t full;

    ValidationQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~ValidationQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

class CompletionSet {
   private:
    set<uint64_t> C;
    pthread_mutex_t mutex;

   public:
    CompletionSet() {
        pthread_mutex_init(&mutex, NULL);
    }

    ~CompletionSet() {
        pthread_mutex_destroy(&mutex);
    }

    void add(uint64_t trans_id) {
        pthread_mutex_lock(&mutex);
        C.insert(trans_id);
        pthread_mutex_unlock(&mutex);
    }

    void clear() {
        pthread_mutex_lock(&mutex);
        C.clear();
        pthread_mutex_unlock(&mutex);
    }

    bool find(uint64_t trans_id) {
        bool find = false;
        pthread_mutex_lock(&mutex);
        if (C.find(trans_id) != C.end()) {
            find = true;
        }
        pthread_mutex_unlock(&mutex);

        return find;
    }

    size_t size() {
        size_t size;
        // pthread_mutex_lock(&mutex);
        size = C.size();
        // pthread_mutex_unlock(&mutex);

        return size;
    }
};

class CoreManager {
private:
    // Track active cores
    std::vector<int> active_cores;
    
    // Map core_id to its simulation and validation threads
    std::unordered_map<int, std::vector<pthread_t>> sim_threads_by_core;
    std::unordered_map<int, std::vector<pthread_t>> val_threads_by_core;
    
    // Communication components pool
    std::vector<ThreadContext> thread_contexts;
    std::vector<bool> context_in_use;
    std::unordered_map<pthread_t, int> thread_to_context_index;
    
    // Configuration
    int sim_threads_per_core;
    int val_threads_per_core;
    int max_available_threads;
    
    // Thread synchronization
    std::mutex core_mutex;
    
    // 创建并启动线程
    pthread_t create_thread(int core_id, bool is_simulation);
    
    // 优雅地停止线程
    void stop_thread(pthread_t tid);

public:
    CoreManager(int sim_per_core, int val_per_core, int max_threads);
    ~CoreManager();
    
    // 初始化特定数量的核心
    void initialize(int num_cores, const std::vector<int>& core_ids = {});
    
    // 初始化具有特定线程配置的核心
    void initialize_with_config(int num_cores, 
                               int sim_threads, 
                               int val_threads, 
                               const std::vector<int>& core_ids = {});
    
    // Get current number of cores
    int get_core_count();
    
    // Get current threads per core
    std::pair<int, int> get_threads_per_core();
    
    // Get maximum available threads
    int get_max_threads();
    
    // 添加单个验证线程到指定核心
    int add_validation_thread(int core_id);
    
    // Add a core with the current thread distribution
    int add_core(int core_id);
    
    // Remove a core (defaults to last core if none specified)
    int remove_core(int core_id = -1);
    
    // Adjust thread counts per core
    int adjust_thread(int d_sim, int d_val);
    
    // 获取活动核心列表 - 为统计模块添加的新方法
    std::vector<int> GetActiveCores();
};


extern CoreManager* g_core_manager;

#endif