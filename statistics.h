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

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "memory_config.pb.h"
#include "memory_config.grpc.pb.h"

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

class MemoryConfigClient {
    public:
        MemoryConfigClient(std::shared_ptr<Channel> channel)
            : stub_(MemoryConfig::NewStub(channel)) {}
    
        // 设置驱逐阈值
        bool SetEvictThreshold(uint32_t threshold, uint32_t& new_threshold);
        
        // 获取当前驱逐阈值
        bool GetEvictThreshold(uint32_t& threshold);
    
    private:
        std::unique_ptr<MemoryConfig::Stub> stub_;
};

#endif // STATISTICS_H
